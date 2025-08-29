// Vercel Serverless Function (Node.js) — POST body carries the filters
// Env vars required:
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

  // --- read JSON body safely (works whether req.body is present or not) ---
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

  // --- build first-page URL with cursor pagination engaged ---
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
  const out = [];
  const seen = new Set();
  let pages = 0;

  try {
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

        // ----- normalize userId -----
        const topUser = Number(s.userId);
        const authUser = Number(s.authorization?.userId);
        const resolvedUserId = Number.isFinite(topUser) && topUser > 0
          ? topUser
          : (Number.isFinite(authUser) && authUser > 0 ? authUser : 0);

        // ----- normalize idTag (optional) -----
        const resolvedIdTag = s.idTag ?? s.authorization?.rfidTagUid ?? null;

        out.push({
          ...s,
          userIdRaw: s.userId ?? null,
          userId: resolvedUserId,        // overwrite with resolved id
          idTagRaw: s.idTag ?? null,
          idTag: resolvedIdTag,          // ensure we have an idTag if possible
        });
      }

      next = page?.links?.next || null;
      pages++;
    }

    res.statusCode = 200;
    return res.json({ count: out.length, data: out });
  } catch (err) {
    res.statusCode = 500;
    return res.json({ error: String(err) });
  }
};
