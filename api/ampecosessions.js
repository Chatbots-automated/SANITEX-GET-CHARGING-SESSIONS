// /api/ampecosessions.js
// Fetch sessions (partner-scoped), then fetch charge points (listing) to map locationId,
// then fetch locations (listing) and enrich each session with site name/address/geo.
// Env:
//   AMPECO_BASE_URL      (e.g. https://cp.ikrautas.lt)
//   AMPECO_PARTNER_TOKEN (either with or without "Bearer " prefix)

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.status(405).json({ error: 'Method Not Allowed. Use POST.' });
    return;
  }

  const BASE = process.env.AMPECO_BASE_URL || 'https://cp.ikrautas.lt';
  let token = process.env.AMPECO_PARTNER_TOKEN || '';
  if (!token) return res.status(500).json({ error: 'Missing AMPECO_PARTNER_TOKEN' });
  if (!/^Bearer\s/i.test(token)) token = `Bearer ${token}`;

  // Parse body safely
  let body = {};
  try {
    if (req.body && typeof req.body === 'object') {
      body = req.body;
    } else {
      let raw = ''; for await (const chunk of req) raw += chunk;
      body = raw ? JSON.parse(raw) : {};
    }
  } catch {
    return res.status(400).json({ error: 'Invalid JSON body' });
  }

  const {
    startedAfter,
    startedBefore,
    endedAfter,
    endedBefore,
    tariffSnapshotId,
    per_page = 100,        // API page size cap
    maxPages = 10000       // safety cap
  } = body;

  const headers = { Authorization: token, Accept: 'application/json' };

  // ------------ 1) Get ALL sessions (cursor pagination) ------------
  const sQS = new URLSearchParams();
  sQS.set('per_page', String(Math.min(Number(per_page) || 100, 100)));
  sQS.set('cursor', ''); // engage cursor pagination
  sQS.set('withAuthorization', 'true'); // to resolve userId/idTag if top-level is 0

  if (startedAfter)  sQS.set('filter[startedAfter]', startedAfter);
  if (startedBefore) sQS.set('filter[startedBefore]', startedBefore);
  if (endedAfter)    sQS.set('filter[endedAfter]', endedAfter);
  if (endedBefore)   sQS.set('filter[endedBefore]', endedBefore);
  if (tariffSnapshotId != null) sQS.set('filter[tariffSnapshotId]', String(tariffSnapshotId));

  let next = `${BASE}/public-api/resources/sessions/v1.0?${sQS.toString()}`;
  const sessions = [];
  const seenSessionIds = new Set();
  let pages = 0;

  while (next && pages < maxPages) {
    const r = await fetch(next, { headers });
    if (!r.ok) {
      const text = await r.text();
      return res.status(502).json({ error: 'AMPECO upstream error (sessions)', status: r.status, body: text, nextTried: next });
    }
    const page = await r.json();
    const data = Array.isArray(page?.data) ? page.data : [];

    for (const s of data) {
      const sid = String(s.id ?? `${Date.now()}-${Math.random()}`);
      if (seenSessionIds.has(sid)) continue;
      seenSessionIds.add(sid);

      const topUser = Number(s.userId);
      const authUser = Number(s.authorization?.userId);
      const resolvedUserId =
        (Number.isFinite(topUser) && topUser > 0) ? topUser :
        (Number.isFinite(authUser) && authUser > 0 ? authUser : 0);

      const resolvedIdTag = s.idTag ?? s.authorization?.rfidTagUid ?? null;
      const wh = (s.energy ?? s.energyConsumption?.total ?? 0);
      const kWh = Number((wh / 1000).toFixed(3));

      sessions.push({
        ...s,
        userIdRaw: s.userId ?? null,
        userId: resolvedUserId,
        idTagRaw: s.idTag ?? null,
        idTag: resolvedIdTag,
        kWh,
      });
    }

    next = page?.links?.next || null;
    pages++;
  }

  // If no sessions, short-circuit
  if (sessions.length === 0) {
    return res.status(200).json({ count: 0, data: [] });
  }

  // ------------ 2) Charge Points / Listing to map {cpId -> {locationId, names...}} ------------
  const wantedCpIds = new Set(
    sessions.map(s => s.chargePointId).filter(id => id !== null && id !== undefined)
  );

  const cpMap = new Map();         // cpId -> cp object (must include locationId)
  const wantedButMissing = new Set(wantedCpIds); // track what’s left to find

  // NOTE: We use the documented listing endpoint and stop when we’ve found all needed CPs.
  let cpNext = `${BASE}/public-api/resources/charge-points/v1.0?per_page=100&cursor=`;
  let cpPages = 0;

  while (cpNext && cpPages < maxPages && wantedButMissing.size > 0) {
    const r = await fetch(cpNext, { headers });
    if (!r.ok) {
      const text = await r.text();
      return res.status(502).json({ error: 'AMPECO upstream error (charge-points listing)', status: r.status, body: text, nextTried: cpNext });
    }
    const page = await r.json();
    const cps = Array.isArray(page?.data) ? page.data : [];

    for (const cp of cps) {
      const id = cp?.id ?? cp?.chargePointId ?? null;
      if (id == null) continue;
      if (wantedButMissing.has(id)) {
        cpMap.set(id, cp);
        wantedButMissing.delete(id);
      }
    }

    cpNext = page?.links?.next || null;
    cpPages++;
  }

  // ------------ 3) Locations / Listing to map {locationId -> location} ------------
  // Collect needed locationIds from cpMap
  const wantedLocIds = new Set(
    Array.from(cpMap.values())
      .map(cp => cp?.locationId)
      .filter(id => id !== null && id !== undefined)
  );

  const locMap = new Map(); // locationId -> location object
  let locNext = `${BASE}/public-api/resources/locations/v1.0?per_page=100&cursor=`;
  let locPages = 0;
  const missingLocs = new Set(wantedLocIds);

  while (locNext && locPages < maxPages && missingLocs.size > 0) {
    const r = await fetch(locNext, { headers });
    if (!r.ok) {
      const text = await r.text();
      return res.status(502).json({ error: 'AMPECO upstream error (locations listing)', status: r.status, body: text, nextTried: locNext });
    }
    const page = await r.json();
    const locs = Array.isArray(page?.data) ? page.data : [];

    for (const loc of locs) {
      const id = loc?.id;
      if (id == null) continue;
      if (missingLocs.has(id)) {
        locMap.set(id, loc);
        missingLocs.delete(id);
      }
    }

    locNext = page?.links?.next || null;
    locPages++;
  }

  // Helpers to pull human fields from CP & Location
  const extractCpFields = (cp) => {
    if (!cp || typeof cp !== 'object') return {};
    return {
      chargePointName: cp.name ?? cp.title ?? cp.reference ?? cp.code ?? null,
      locationId: cp.locationId ?? null,
    };
  };

  const extractLocationFields = (loc) => {
    if (!loc || typeof loc !== 'object') return {};
    const name = loc.name ?? loc.title ?? null;
    const address = loc.address ?? loc.siteAddress ?? loc.location?.address ?? null;
    const line1 = address?.line1 ?? address?.addressLine1 ?? address?.street ?? address?.line ?? null;
    const city = address?.city ?? address?.town ?? address?.locality ?? null;
    const country = address?.country ?? address?.countryCode ?? null;
    const geo = loc.geoposition ?? loc.geo ?? loc.location?.geoposition ?? {};
    const latitude = geo?.latitude ?? geo?.lat ?? null;
    const longitude = geo?.longitude ?? geo?.lng ?? null;
    return {
      locationName: name,
      addressLine1: line1,
      city,
      country,
      latitude,
      longitude,
    };
  };

  // ------------ 4) Enrich sessions with CP + Location info ------------
  const enriched = sessions.map((s) => {
    const cp = cpMap.get(s.chargePointId);
    const { chargePointName, locationId } = extractCpFields(cp);
    const loc = locMap.get(locationId);
    const locFields = extractLocationFields(loc);
    return {
      ...s,
      chargePointName: chargePointName ?? null,
      locationId: locationId ?? null,
      ...locFields,
    };
  });

  res.status(200).json({ count: enriched.length, data: enriched });
};
