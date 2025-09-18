// /api/ampecosessions.js
// Sessions + CP/Location + Holder name + EVSE details (no v1.0 EVSE calls)

module.exports = async (req, res) => {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method Not Allowed. Use POST.' });

  const BASE = process.env.AMPECO_BASE_URL || 'https://cp.ikrautas.lt';
  let token = process.env.AMPECO_PARTNER_TOKEN || '';
  if (!token) return res.status(500).json({ error: 'Missing AMPECO_PARTNER_TOKEN' });
  if (!/^Bearer\s/i.test(token)) token = `Bearer ${token}`;
  const headers = { Authorization: token, Accept: 'application/json' };

  // ---- parse body ----
  let body = {};
  try {
    if (req.body && typeof req.body === 'object') body = req.body;
    else {
      let raw = ''; for await (const c of req) raw += c;
      body = raw ? JSON.parse(raw) : {};
    }
  } catch {
    return res.status(400).json({ error: 'Invalid JSON body' });
  }

  const {
    startedAfter, startedBefore, endedAfter, endedBefore,
    tariffSnapshotId, per_page = 100, maxPages = 10000,
  } = body;

  /* ========== 1) Sessions ========== */
  const sQS = new URLSearchParams();
  sQS.set('per_page', String(Math.min(Number(per_page) || 100, 100)));
  sQS.set('cursor', '');
  sQS.set('withAuthorization', 'true');
  if (startedAfter)  sQS.set('filter[startedAfter]', startedAfter);
  if (startedBefore) sQS.set('filter[startedBefore]', startedBefore);
  if (endedAfter)    sQS.set('filter[endedAfter]', endedAfter);
  if (endedBefore)   sQS.set('filter[endedBefore]', endedBefore);
  if (tariffSnapshotId != null) sQS.set('filter[tariffSnapshotId]', String(tariffSnapshotId));

  let next = `${BASE}/public-api/resources/sessions/v1.0?${sQS.toString()}`;
  const sessions = []; const seen = new Set(); let pages = 0;

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
      if (seen.has(sid)) continue; seen.add(sid);
      const topUser = Number(s.userId);
      const authUser = Number(s.authorization?.userId);
      const resolvedUserId =
        (Number.isFinite(topUser) && topUser > 0) ? topUser :
        (Number.isFinite(authUser) && authUser > 0 ? authUser : 0);
      const resolvedIdTag = s.idTag ?? s.authorization?.rfidTagUid ?? null;
      const wh = (s.energy ?? s.energyConsumption?.total ?? 0);
      const kWh = Number((wh / 1000).toFixed(3));
      sessions.push({ ...s, userIdRaw: s.userId ?? null, userId: resolvedUserId, idTagRaw: s.idTag ?? null, idTag: resolvedIdTag, kWh });
    }
    next = page?.links?.next || null;
    pages++;
  }
  if (!sessions.length) return res.status(200).json({ count: 0, data: [] });

  /* ========== 2) Charge Points listing (grab locationId + embedded evses if present) ========== */
  const wantedCpIds = new Set(sessions.map(s => s.chargePointId).filter(v => v != null));
  const cpMap = new Map(); const missingCp = new Set(wantedCpIds);
  let cpNext = `${BASE}/public-api/resources/charge-points/v1.0?per_page=100&cursor=`;
  pages = 0;

  // EVSE cache populated from CP payloads if available
  const evseMap = new Map(); // evseId -> evse object

  while (cpNext && pages < maxPages && missingCp.size > 0) {
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
        // If CP includes `evses`, index them by id to avoid extra calls.
        const list = Array.isArray(cp?.evses) ? cp.evses : [];
        for (const evse of list) {
          const evseId = evse?.id ?? evse?.evseId ?? null;
          if (evseId != null && !evseMap.has(evseId)) evseMap.set(evseId, evse);
        }
        missingCp.delete(id);
      }
    }
    cpNext = page?.links?.next || null;
    pages++;
  }

  /* ========== 3) Locations listing (address/geo) ========== */
  const wantedLocIds = new Set(
    Array.from(cpMap.values()).map(cp => cp?.locationId).filter(v => v != null)
  );
  const locMap = new Map(); const missingLocs = new Set(wantedLocIds);
  let locNext = `${BASE}/public-api/resources/locations/v1.0?per_page=100&cursor=`;
  pages = 0;

  while (locNext && pages < maxPages && missingLocs.size > 0) {
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
      if (missingLocs.has(id)) { locMap.set(id, loc); missingLocs.delete(id); }
    }
    locNext = page?.links?.next || null;
    pages++;
  }

  const extractCpFields = (cp) => ({
    chargePointName: cp?.name ?? cp?.title ?? cp?.reference ?? cp?.code ?? null,
    locationId: cp?.locationId ?? null,
  });

  const extractLocationFields = (loc) => {
    const name = loc?.name ?? loc?.title ?? null;
    const address = loc?.address ?? loc?.siteAddress ?? loc?.location?.address ?? null;
    const line1 = address?.line1 ?? address?.addressLine1 ?? address?.street ?? address?.line ?? null;
    const city = address?.city ?? address?.town ?? address?.locality ?? null;
    const country = address?.country ?? address?.countryCode ?? null;
    const geo = loc?.geoposition ?? loc?.geo ?? loc?.location?.geoposition ?? {};
    const latitude = geo?.latitude ?? geo?.lat ?? null;
    const longitude = geo?.longitude ?? geo?.lng ?? null;
    return { locationName: name, addressLine1: line1, city, country, latitude, longitude };
  };

  /* ========== 4) Holder name (users + id-tags fallback) ========== */
  const userIds = new Set(sessions.map(s => s.userId).filter(n => Number.isFinite(n) && n > 0));
  const idTagsNeedingLookup = new Set(
    sessions.filter(s => (!s.idTagLabel || String(s.idTagLabel).trim() === '') && s.idTag).map(s => s.idTag)
  );

  const userMap = new Map();
  const USER_CONCURRENCY = 8;
  const fetchUser = async (uid) => {
    try {
      const r = await fetch(`${BASE}/public-api/resources/users/v1.0/${uid}`, { headers });
      if (!r.ok) return;
      const payload = await r.json(); const u = payload?.data ?? payload;
      const first = u?.firstName ?? u?.firstname ?? null;
      const last  = u?.lastName  ?? u?.lastname  ?? null;
      const email = u?.email ?? null;
      const name  = u?.name ?? ([first, last].filter(Boolean).join(' ') || null);
      userMap.set(uid, { firstName: first, lastName: last, email, name });
    } catch {}
  };
  const arrU = Array.from(userIds);
  for (let i = 0; i < arrU.length; i += USER_CONCURRENCY) {
    await Promise.all(arrU.slice(i, i + USER_CONCURRENCY).map(fetchUser));
  }

  // id-tags -> userId -> user
  const tagToUserId = new Map();
  const arrTags = Array.from(idTagsNeedingLookup);
  for (let i = 0; i < arrTags.length; i += 8) {
    await Promise.all(arrTags.slice(i, i + 8).map(async (uid) => {
      const qs = new URLSearchParams(); qs.set('filter[uid]', uid); qs.set('per_page', '1');
      try {
        const r = await fetch(`${BASE}/public-api/resources/id-tags/v2.0?${qs.toString()}`, { headers });
        if (!r.ok) return;
        const payload = await r.json();
        const row = Array.isArray(payload?.data) ? payload.data[0] : null;
        const uId = row?.userId ?? row?.user?.id ?? null;
        if (uId != null) tagToUserId.set(uid, uId);
      } catch {}
    }));
  }
  const extraUserIds = Array.from(tagToUserId.values()).filter(uId => !userMap.has(uId));
  for (let i = 0; i < extraUserIds.length; i += USER_CONCURRENCY) {
    await Promise.all(extraUserIds.slice(i, i + USER_CONCURRENCY).map(fetchUser));
  }

  /* ========== 5) EVSE details WITHOUT v1.0 ==========
     Strategy:
       a) Use EVSEs embedded in charge-points listing (already indexed).
       b) For any evseId still missing, call: /public-api/resources/charge-points/v1.0/{cpId}/evses
       c) If some are STILL missing, try global listing /public-api/resources/evses/v2.0
  =================================================== */
  const wantedEvseIds = new Set(sessions.map(s => s.evseId).filter(v => v != null));

  // (b) per-charge-point EVSE listing for missing
  const missingEvse = new Set([...wantedEvseIds].filter(id => !evseMap.has(id)));
  const CP_EVSE_CONCURRENCY = 6;
  const cpIdsForEvse = Array.from(new Set(sessions.map(s => s.chargePointId).filter(v => v != null)));

  for (let i = 0; i < cpIdsForEvse.length && missingEvse.size > 0; i += CP_EVSE_CONCURRENCY) {
    const chunk = cpIdsForEvse.slice(i, i + CP_EVSE_CONCURRENCY);
    await Promise.all(chunk.map(async (cpId) => {
      try {
        let url = `${BASE}/public-api/resources/charge-points/v1.0/${cpId}/evses?per_page=100&cursor=`;
        // page through per-CP evses
        while (url && missingEvse.size > 0) {
          const r = await fetch(url, { headers });
          if (!r.ok) break;
          const payload = await r.json();
          const evses = Array.isArray(payload?.data) ? payload.data : [];
          for (const e of evses) {
            const id = e?.id ?? e?.evseId ?? null;
            if (id != null) {
              evseMap.set(id, e);
              missingEvse.delete(id);
            }
          }
          url = payload?.links?.next || null;
        }
      } catch {}
    }));
  }

  // (c) final fallback: global EVSE listing v2.x (NOT v1.0)
  if (missingEvse.size > 0) {
    let url = `${BASE}/public-api/resources/evses/v2.0?per_page=100&cursor=`;
    let tries = 0;
    while (url && tries < maxPages && missingEvse.size > 0) {
      const r = await fetch(url, { headers });
      if (!r.ok) break; // if forbidden or 404, just stop
      const payload = await r.json();
      const evses = Array.isArray(payload?.data) ? payload.data : [];
      for (const e of evses) {
        const id = e?.id ?? e?.evseId ?? null;
        if (id != null && missingEvse.has(id)) {
          evseMap.set(id, e);
          missingEvse.delete(id);
        }
      }
      url = payload?.links?.next || null;
      tries++;
    }
  }

  const extractEvseFields = (evse) => {
    if (!evse || typeof evse !== 'object') return { evseType: null, connectorStandards: null, maxPowerKw: null };
    const evseType =
      evse.type ?? evse.evseType ?? evse.currentType ?? evse.powerType ?? evse.dcAc ?? null;

    const connectors = evse.connectors || evse.connectorList || [];
    const connStandards = Array.from(new Set(
      connectors.map(c => c?.standard || c?.type || c?.connectorType || c?.format || null).filter(Boolean)
    ));
    const connectorStandards = connStandards.length ? connStandards : null;

    let maxPowerKw = null;
    const pKw = evse.maxPowerKw ?? evse.powerKw ?? evse.power ?? null;
    const pW  = evse.maxPowerW  ?? evse.powerW  ?? null;
    if (Number.isFinite(Number(pKw))) maxPowerKw = Number(pKw);
    else if (Number.isFinite(Number(pW))) maxPowerKw = Number(pW) / 1000;

    return { evseType, connectorStandards, maxPowerKw: maxPowerKw ?? null };
  };

  /* ========== 6) Build response ========== */
  const enriched = sessions.map((s) => {
    const cp = cpMap.get(s.chargePointId);
    const { chargePointName, locationId } = extractCpFields(cp);
    const loc = locMap.get(locationId);
    const locFields = extractLocationFields(loc);

    const userInfo =
      (s.userId && userMap.get(s.userId)) ||
      null;

    const holderName =
      (s.idTagLabel && String(s.idTagLabel).trim()) ||
      (userInfo?.name) ||
      (userInfo ? [userInfo.firstName, userInfo.lastName].filter(Boolean).join(' ') : '') ||
      (userInfo?.email) ||
      null;

    const evse = evseMap.get(s.evseId);
    const { evseType, connectorStandards, maxPowerKw } = extractEvseFields(evse);

    return {
      ...s,
      chargePointName: chargePointName ?? null,
      locationId: locationId ?? null,
      holderName,
      holderEmail: userInfo?.email ?? null,
      evseType,
      connectorStandards,
      maxPowerKw,
      ...locFields,
    };
  });

  return res.status(200).json({ count: enriched.length, data: enriched });
};
