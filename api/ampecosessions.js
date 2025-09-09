// Vercel Serverless Function (Node.js) — sessions + charge point location enrichment
// Env vars:
//   AMPECO_BASE_URL      e.g. https://cp.ikrautas.lt
//   AMPECO_PARTNER_TOKEN e.g. "Bearer 8fb5...aab6" (add Bearer if missing)

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.statusCode = 405;
    return res.json({ error: 'Method Not Allowed. Use POST.' });
  }

  const BASE = process.env.AMPECO_BASE_URL || 'https://cp.ikrautas.lt';
  let token = process.env.AMPECO_PARTNER_TOKEN || '';
  if (!token) {
    res.statusCode = 500;
    return res.json({ error: 'Missing AMPECO_PARTNER_TOKEN' });
  }
  if (!/^Bearer\s/i.test(token)) token = `Bearer ${token}`;

  // ---- read JSON body safely ----
  let body = {};
  try {
    if (req.body && typeof req.body === 'object') {
      body = req.body;
    } else {
      let raw = '';
      for await (const chunk of req) raw += chunk;
      body = raw ? JSON.parse(raw) : {};
    }
  } catch {
    res.statusCode = 400;
    return res.json({ error: 'Invalid JSON body' });
  }

  const {
    startedAfter,
    startedBefore,
    endedAfter,
    endedBefore,
    tariffSnapshotId,
    per_page = 100,      // API max is 100
    maxPages = 10000     // big safety cap
  } = body;

  // ---- first-page URL (cursor pagination engaged) ----
  const qs = new URLSearchParams();
  qs.set('per_page', String(Math.min(Number(per_page) || 100, 100)));
  qs.set('cursor', ''); // engage cursor pagination
  qs.set('withAuthorization', 'true'); // include authorization to resolve userId/idTag

  if (startedAfter)  qs.set('filter[startedAfter]', startedAfter);
  if (startedBefore) qs.set('filter[startedBefore]', startedBefore);
  if (endedAfter)    qs.set('filter[endedAfter]', endedAfter);
  if (endedBefore)   qs.set('filter[endedBefore]', endedBefore);
  if (tariffSnapshotId != null) qs.set('filter[tariffSnapshotId]', String(tariffSnapshotId));

  let next = `${BASE}/public-api/resources/sessions/v1.0?${qs.toString()}`;
  const sessions = [];
  const seen = new Set();
  let pages = 0;

  try {
    // ---- fetch ALL sessions ----
    while (next && pages < maxPages) {
      const resp = await fetch(next, {
        headers: { Authorization: token, Accept: 'application/json' },
      });

      if (!resp.ok) {
        const text = await resp.text();
        res.statusCode = 502;
        return res.json({ error: 'AMPECO upstream error', status: resp.status, body: text, nextTried: next });
      }

      const page = await resp.json();
      const data = Array.isArray(page?.data) ? page.data : [];

      for (const s of data) {
        const uniqueId = String(s.id ?? `${Date.now()}-${Math.random()}`);
        if (seen.has(uniqueId)) continue;
        seen.add(uniqueId);

        // resolve userId
        const topUser = Number(s.userId);
        const authUser = Number(s.authorization?.userId);
        const resolvedUserId =
          (Number.isFinite(topUser) && topUser > 0) ? topUser :
          (Number.isFinite(authUser) && authUser > 0 ? authUser : 0);

        // resolve idTag
        const resolvedIdTag = s.idTag ?? s.authorization?.rfidTagUid ?? null;

        // kWh from energy (Wh → kWh); fallback to energyConsumption.total
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

    // ---- collect unique chargePointIds to enrich with location ----
    const cpIds = Array.from(
      new Set(
        sessions
          .map(s => s.chargePointId)
          .filter(id => id !== null && id !== undefined)
      )
    );

    // Helper: extract best-effort location fields from a charge point object
    const extractLocation = (cp) => {
      if (!cp || typeof cp !== 'object') return {};
      const address =
        cp.address ??
        cp.location?.address ??
        cp.site?.address ??
        cp.siteAddress ??
        null;

      const line1 =
        address?.line1 ?? address?.addressLine1 ?? address?.street ??
        address?.line ?? address?.street1 ?? null;

      const city =
        address?.city ?? address?.town ?? address?.locality ?? null;

      const country =
        address?.country ?? address?.countryCode ?? null;

      const latitude =
        cp.latitude ?? cp.lat ?? cp.location?.latitude ??
        cp.location?.lat ?? cp.gpsLat ?? cp.geo?.lat ?? null;

      const longitude =
        cp.longitude ?? cp.lon ?? cp.lng ?? cp.location?.longitude ??
        cp.location?.lng ?? cp.gpsLng ?? cp.geo?.lng ?? null;

      const chargePointName =
        cp.name ?? cp.title ?? cp.reference ?? cp.code ?? null;

      const siteName = cp.site?.name ?? cp.location?.name ?? null;

      return { chargePointName, siteName, addressLine1: line1, city, country, latitude, longitude };
    };

    // ---- fetch charge point details (per-id; limited concurrency) ----
    const headers = { Authorization: token, Accept: 'application/json' };
    const cpMap = new Map();
    const CONCURRENCY = 8;

    for (let i = 0; i < cpIds.length; i += CONCURRENCY) {
      const chunk = cpIds.slice(i, i + CONCURRENCY);
      await Promise.all(
        chunk.map(async (id) => {
          try {
            const url = `${BASE}/public-api/resources/charge-points/v1.0/${id}`;
            const r = await fetch(url, { headers });
            if (!r.ok) return; // skip if not accessible
            const payload = await r.json();
            const cp = payload?.data ?? payload; // tolerate both shapes
            cpMap.set(id, cp);
          } catch {
            // ignore errors per charge point
          }
        })
      );
    }

    // ---- enrich sessions with charge point location/name fields ----
    const enriched = sessions.map((s) => {
      const cp = cpMap.get(s.chargePointId);
      const loc = extractLocation(cp);
      return {
        ...s,
        ...loc,
        chargePoint: cp || null, // keep full object (optional)
      };
    });

    res.statusCode = 200;
    return res.json({ count: enriched.length, data: enriched });
  } catch (err) {
    res.statusCode = 500;
    return res.json({ error: String(err) });
  }
};
