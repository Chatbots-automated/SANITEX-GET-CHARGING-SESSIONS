// /api/ampecosessions.js
// Sessions (partner-scoped) + CP/Location enrichment + Holder name via Users/Read,
// fallback via Id Tags / Listing (by UID).
// Env:
//   AMPECO_BASE_URL      e.g. https://cp.ikrautas.lt
//   AMPECO_PARTNER_TOKEN e.g. "Bearer 8fb5...aab6" (prefix is auto-added if missing)

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method Not Allowed. Use POST.' });
  }

  const BASE = process.env.AMPECO_BASE_URL || 'https://cp.ikrautas.lt';
  let token = process.env.AMPECO_PARTNER_TOKEN || '';
  if (!token) return res.status(500).json({ error: 'Missing AMPECO_PARTNER_TOKEN' });
  if (!/^Bearer\s/i.test(token)) token = `Bearer ${token}`;

  // ---- parse body ----
  let body = {};
  try {
    if (req.body && typeof req.body === 'object') {
      body = req.body;
    } else {
      let raw = ''; for await (const c of req) raw += c;
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
    per_page = 100,
    maxPages = 10000,
  } = body;

  const headers = { Authorization: token, Accept: 'application/json' };

  /* ========== 1) Sessions (cursor pagination) ========== */
  const sQS = new URLSearchParams();
  sQS.set('per_page', String(Math.min(Number(per_page) || 100, 100)));
  sQS.set('cursor', '');                       // engage cursor
  sQS.set('withAuthorization', 'true');        // to resolve userId/idTag when top-level is 0
  if (startedAfter)  sQS.set('filter[startedAfter]', startedAfter);
  if (startedBefore) sQS.set('filter[startedBefore]', startedBefore);
  if (endedAfter)    sQS.set('filter[endedAfter]', endedAfter);
  if (endedBefore)   sQS.set('filter[endedBefore]', endedBefore);
  if (tariffSnapshotId != null) sQS.set('filter[tariffSnapshotId]', String(tariffSnapshotId));

  let next = `${BASE}/public-api/resources/sessions/v1.0?${sQS.toString()}`;
  const sessions = [];
  const seen = new Set();
  let pages = 0;

  while (next && pages < maxPages) {
    const r = await fetch(next, { headers });
    if (!r.ok) {
      const text = await r.text().catch(() => '');
      return res.status(r.status).json({ stage: 'sessions', upstreamStatus: r.status, url: next, body: text });
    }
    const page = await r.json();
    const data = Array.isArray(page?.data) ? page.data : [];

    for (const s of data) {
      const sid = String(s.id ?? `${Date.now()}-${Math.random()}`);
      if (seen.has(sid)) continue;
      seen.add(sid);

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

  if (sessions.length === 0) {
    return res.status(200).json({ count: 0, data: [] });
  }

  /* ========== 2) Charge Points listing to map locationId ========== */
  const wantedCpIds = new Set(
    sessions.map(s => s.chargePointId).filter(id => id !== null && id !== undefined)
  );
  const cpMap = new Map();
  const missingCp = new Set(wantedCpIds);
  let cpNext = `${BASE}/public-api/resources/charge-points/v1.0?per_page=100&cursor=`;
  let cpPages = 0;

  while (cpNext && cpPages < maxPages && missingCp.size > 0) {
    const r = await fetch(cpNext, { headers });
    if (!r.ok) {
      const text = await r.text().catch(() => '');
      return res.status(r.status).json({ stage: 'charge-points', upstreamStatus: r.status, url: cpNext, body: text });
    }
    const page = await r.json();
    const cps = Array.isArray(page?.data) ? page.data : [];
    for (const cp of cps) {
      const id = cp?.id ?? cp?.chargePointId ?? null;
      if (id == null) continue;
      if (missingCp.has(id)) {
        cpMap.set(id, cp);
        missingCp.delete(id);
      }
    }
    cpNext = page?.links?.next || null;
    cpPages++;
  }

  /* ========== 3) Locations listing to resolve site address/geo ========== */
  const wantedLocIds = new Set(
    Array.from(cpMap.values())
      .map(cp => cp?.locationId)
      .filter(id => id !== null && id !== undefined)
  );

  const locMap = new Map();
  const missingLocs = new Set(wantedLocIds);
  let locNext = `${BASE}/public-api/resources/locations/v1.0?per_page=100&cursor=`;
  let locPages = 0;

  while (locNext && locPages < maxPages && missingLocs.size > 0) {
    const r = await fetch(locNext, { headers });
    if (!r.ok) {
      const text = await r.text().catch(() => '');
      return res.status(r.status).json({ stage: 'locations', upstreamStatus: r.status, url: locNext, body: text });
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

  const extractCpFields = (cp) => {
    if (!cp || typeof cp !== 'object') return { chargePointName: null, locationId: null };
    return {
      chargePointName: cp.name ?? cp.title ?? cp.reference ?? cp.code ?? null,
      locationId: cp.locationId ?? null,
    };
  };

  const extractLocationFields = (loc) => {
    if (!loc || typeof loc !== 'object') {
      return { locationName: null, addressLine1: null, city: null, country: null, latitude: null, longitude: null };
    }
    const name = loc.name ?? loc.title ?? null;
    const address = loc.address ?? loc.siteAddress ?? loc.location?.address ?? null;
    const line1 = address?.line1 ?? address?.addressLine1 ?? address?.street ?? address?.line ?? null;
    const city = address?.city ?? address?.town ?? address?.locality ?? null;
    const country = address?.country ?? address?.countryCode ?? null;
    const geo = loc.geoposition ?? loc.geo ?? loc.location?.geoposition ?? {};
    const latitude = geo?.latitude ?? geo?.lat ?? null;
    const longitude = geo?.longitude ?? geo?.lng ?? null;
    return { locationName: name, addressLine1: line1, city, country, latitude, longitude };
  };

  /* ========== 4) Holder name enrichment ========== */
  // First pass: collect userIds>0 and idTags without a label.
  const userIds = new Set(sessions.map(s => s.userId).filter(n => Number.isFinite(n) && n > 0));
  const idTagsNeedingLookup = new Set(
    sessions
      .filter(s => (!s.idTagLabel || String(s.idTagLabel).trim() === '') && s.idTag)
      .map(s => s.idTag)
  );

  // 4a) Fetch Users / Read individually (safe & reliable)
  const userMap = new Map(); // userId -> { firstName, lastName, email, name }
  const USER_CONCURRENCY = 8;
  const userArray = Array.from(userIds);

  for (let i = 0; i < userArray.length; i += USER_CONCURRENCY) {
    const chunk = userArray.slice(i, i + USER_CONCURRENCY);
    await Promise.all(chunk.map(async (uid) => {
      const url = `${BASE}/public-api/resources/users/v1.0/${uid}`;
      try {
        const r = await fetch(url, { headers });
        if (!r.ok) return;
        const payload = await r.json();
        const u = payload?.data ?? payload;
        const first = u?.firstName ?? u?.firstname ?? null;
        const last  = u?.lastName  ?? u?.lastname  ?? null;
        const email = u?.email ?? null;
        const name  = u?.name ?? ([first, last].filter(Boolean).join(' ') || null);
        userMap.set(uid, { firstName: first, lastName: last, email, name });
      } catch { /* ignore per-user errors */ }
    }));
  }

  // 4b) Fallback: Id Tags / Listing by UID to discover userId, then fetch that user
  // (Some partner-scoped tokens may not allow this; we ignore failures.)
  const tagToUserId = new Map(); // uid -> userId
  const TAG_CONCURRENCY = 8;
  const tagArray = Array.from(idTagsNeedingLookup);

  for (let i = 0; i < tagArray.length; i += TAG_CONCURRENCY) {
    const chunk = tagArray.slice(i, i + TAG_CONCURRENCY);
    await Promise.all(chunk.map(async (uid) => {
      const qs = new URLSearchParams();
      // Docs show Id Tags v2.0; filtering key commonly 'uid'
      qs.set('filter[uid]', uid);
      qs.set('per_page', '1');
      const url = `${BASE}/public-api/resources/id-tags/v2.0?${qs.toString()}`;
      try {
        const r = await fetch(url, { headers });
        if (!r.ok) return;
        const payload = await r.json();
        const row = Array.isArray(payload?.data) ? payload.data[0] : null;
        const uId = row?.userId ?? row?.user?.id ?? null;
        if (uId != null) tagToUserId.set(uid, uId);
      } catch { /* ignore */ }
    }));
  }

  // Fetch any users discovered from id-tags that we still don't have
  const extraUserIds = Array.from(tagToUserId.values()).filter(uId => !userMap.has(uId));
  for (let i = 0; i < extraUserIds.length; i += USER_CONCURRENCY) {
    const chunk = extraUserIds.slice(i, i + USER_CONCURRENCY);
    await Promise.all(chunk.map(async (uid) => {
      const url = `${BASE}/public-api/resources/users/v1.0/${uid}`;
      try {
        const r = await fetch(url, { headers });
        if (!r.ok) return;
        const payload = await r.json();
        const u = payload?.data ?? payload;
        const first = u?.firstName ?? u?.firstname ?? null;
        const last  = u?.lastName  ?? u?.lastname  ?? null;
        const email = u?.email ?? null;
        const name  = u?.name ?? ([first, last].filter(Boolean).join(' ') || null);
        userMap.set(uid, { firstName: first, lastName: last, email, name });
      } catch { /* ignore */ }
    }));
  }

  /* ========== 5) Final enrichment & response ========== */
  const enriched = sessions.map((s) => {
    const cp = cpMap.get(s.chargePointId);
    const { chargePointName, locationId } = extractCpFields(cp);
    const loc = locMap.get(locationId);
    const locFields = extractLocationFields(loc);

    // Holder name: prefer idTagLabel; else userMap; else email; else null
    const userInfo =
      (s.userId && userMap.get(s.userId)) ||
      (s.idTag && tagToUserId.has(s.idTag) && userMap.get(tagToUserId.get(s.idTag))) ||
      null;

    const holderName =
      (s.idTagLabel && String(s.idTagLabel).trim()) ||
      (userInfo?.name) ||
      (userInfo ? [userInfo.firstName, userInfo.lastName].filter(Boolean).join(' ') : '') ||
      (userInfo?.email) ||
      null;

    return {
      ...s,
      chargePointName: chargePointName ?? null,
      locationId: locationId ?? null,
      holderName,
      holderEmail: userInfo?.email ?? null,
      ...locFields,
    };
  });

  return res.status(200).json({ count: enriched.length, data: enriched });
};
