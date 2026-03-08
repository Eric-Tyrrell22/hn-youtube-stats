const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
const Database = require('better-sqlite3');

const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;
if (!YOUTUBE_API_KEY) { console.error('Missing YOUTUBE_API_KEY env var'); process.exit(1); }

const QUICK = process.argv.includes('--quick');
// 2015-01-01T00:00:00Z to 2025-12-31T23:59:59Z
const YEAR_END = 1767225599;
const YEAR_START = QUICK ? YEAR_END - 60 * 24 * 60 * 60 : 1420070400;
const PAGE_SIZE = 1000;
const WINDOW_SEC = 14 * 24 * 60 * 60;

// 2022-01-01T00:00:00Z
const MIN_CREATED = 1640995200;
const MIN_KARMA = 1000;

const YOUTUBE_RE = /(?:https?:\/\/)?(?:www\.|m\.)?(?:youtube\.com\/watch\?(?:[^"'\s]*&)?v=|youtu\.be\/)([A-Za-z0-9_-]{11})/g;

function decodeHtml(str) {
  return str
    .replace(/&#x2F;/g, '/')
    .replace(/&amp;/g, '&')
    .replace(/&#x27;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

async function fetchWindow(start, end, page) {
  const url = `https://hn.algolia.com/api/v1/search_by_date?tags=comment&query=youtube.com&hitsPerPage=${PAGE_SIZE}&page=${page}&numericFilters=created_at_i>${start},created_at_i<${end}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function openDb() {
  const db = new Database('users.db');
  db.exec(`CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    created INTEGER,
    karma INTEGER,
    fetched_at INTEGER
  )`);
  return db;
}

async function fetchUser(username) {
  const res = await fetch(`https://hacker-news.firebaseio.com/v0/user/${username}.json`);
  if (!res.ok) return null;
  return res.json();
}

async function withConcurrency(items, concurrency, fn) {
  const results = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    results.push(...await Promise.all(batch.map(fn)));
  }
  return results;
}

async function main() {
  // Phase 1: collect all comments with YouTube links
  const windows = [];
  for (let t = YEAR_START; t < YEAR_END; t += WINDOW_SEC) {
    windows.push([t, Math.min(t + WINDOW_SEC, YEAR_END)]);
  }

  console.error(`Phase 1: fetching ${windows.length} time windows...`);

  // author -> [{videoId, storyTitle, storyId, date}]
  const commentsByAuthor = new Map();
  // videoId -> [{author, storyTitle, storyId, date}]  (raw events, pre-filter)
  const commentEvents = new Map();

  for (let i = 0; i < windows.length; i++) {
    const [start, end] = windows[i];
    process.stderr.write(`\r  Window ${i + 1}/${windows.length}`);
    const data = await fetchWindow(start, end, 0);
    for (const hit of data.hits) {
      const text = decodeHtml(hit.comment_text ?? '');
      const ids = [...new Set([...text.matchAll(YOUTUBE_RE)].map(m => m[1]))];
      if (ids.length === 0) continue;
      const author = hit.author;
      const date = hit.created_at?.slice(0, 7) ?? ''; // "YYYY-MM"
      if (!commentsByAuthor.has(author)) commentsByAuthor.set(author, []);
      for (const id of ids) {
        commentsByAuthor.get(author).push({ videoId: id, storyTitle: hit.story_title ?? '', storyId: hit.story_id ?? null, date });
        if (!commentEvents.has(id)) commentEvents.set(id, []);
        commentEvents.get(id).push({ author, commentId: hit.objectID, storyTitle: hit.story_title ?? '', storyId: hit.story_id ?? null, date });
      }
    }
  }

  const allUsers = [...commentsByAuthor.keys()];
  console.error(`\nPhase 1 done. ${allUsers.length} unique users.`);

  // Phase 2: fetch user profiles, using SQLite cache
  const db = openDb();
  const getCached = db.prepare('SELECT created, karma FROM users WHERE username = ?');
  const upsert = db.prepare('INSERT OR REPLACE INTO users (username, created, karma, fetched_at) VALUES (?, ?, ?, ?)');

  const uncached = allUsers.filter(u => !getCached.get(u));
  console.error(`Phase 2: fetching ${uncached.length} uncached user profiles (${allUsers.length - uncached.length} cached)...`);

  let fetched = 0;
  await withConcurrency(uncached, 20, async (username) => {
    const profile = await fetchUser(username);
    const now = Math.floor(Date.now() / 1000);
    upsert.run(username, profile?.created ?? null, profile?.karma ?? null, now);
    fetched++;
    if (fetched % 200 === 0) process.stderr.write(`\r  ${fetched}/${uncached.length} fetched`);
  });
  console.error(`\nPhase 2 done.`);

  const qualifiedUsers = new Set(
    allUsers.filter(u => {
      const row = getCached.get(u);
      return row && row.created < MIN_CREATED && row.karma >= MIN_KARMA;
    })
  );

  console.error(`Qualified users (pre-2022, karma >= ${MIN_KARMA}): ${qualifiedUsers.size}/${allUsers.length}`);

  // Phase 3: count video references from qualified users only, one ref per (videoId, storyId)
  const counts = new Map();
  const seenStory = new Set(); // "videoId:storyId"
  for (const [author, refs] of commentsByAuthor) {
    if (!qualifiedUsers.has(author)) continue;
    for (const { videoId, storyTitle, storyId } of refs) {
      const key = `${videoId}:${storyId}`;
      if (seenStory.has(key)) continue;
      seenStory.add(key);
      if (!counts.has(videoId)) counts.set(videoId, { count: 0, storyTitles: new Set(), storyIds: new Set() });
      const entry = counts.get(videoId);
      entry.count++;
      if (storyTitle) entry.storyTitles.add(storyTitle);
      if (storyId) entry.storyIds.add(storyId);
    }
  }

  const topComments = [...counts.entries()]
    .filter(([, data]) => data.count > 2)
    .sort((a, b) => b[1].count - a[1].count);

  // Phase 3b: scrape HN stories (direct posts) with YouTube URLs, same user filter
  console.error('Phase 3b: fetching HN story windows...');
  const postsByVideo = new Map(); // videoId -> { count, totalScore, storyTitles, storyIds }
  const postEvents = new Map();   // videoId -> [{author, storyTitle, storyId, date, score}]
  const seenPost = new Set();     // "videoId:storyId"

  const storyAuthorsSeen = new Set();
  for (let i = 0; i < windows.length; i++) {
    const [start, end] = windows[i];
    process.stderr.write(`\r  Window ${i + 1}/${windows.length}`);
    const res = await fetch(`https://hn.algolia.com/api/v1/search_by_date?tags=story&query=youtube.com&hitsPerPage=${PAGE_SIZE}&page=0&numericFilters=created_at_i>${start},created_at_i<${end}`);
    const data = await res.json();
    for (const hit of data.hits) {
      storyAuthorsSeen.add(hit.author);
      if (!qualifiedUsers.has(hit.author)) continue;
      const decoded = decodeHtml(hit.url ?? '');
      const matches = [...decoded.matchAll(YOUTUBE_RE)];
      if (matches.length === 0) continue;
      const videoId = matches[0][1];
      const key = `${videoId}:${hit.objectID}`;
      if (seenPost.has(key)) continue;
      seenPost.add(key);
      const date = hit.created_at?.slice(0, 7) ?? '';
      if (!postsByVideo.has(videoId)) postsByVideo.set(videoId, { count: 0, totalScore: 0, storyTitles: new Set(), storyIds: new Set() });
      const entry = postsByVideo.get(videoId);
      entry.count++;
      entry.totalScore += hit.points ?? 0;
      if (hit.title) entry.storyTitles.add(hit.title);
      entry.storyIds.add(hit.objectID);
      if (!postEvents.has(videoId)) postEvents.set(videoId, []);
      postEvents.get(videoId).push({ author: hit.author, storyTitle: hit.title ?? '', storyId: hit.objectID, date, score: hit.points ?? 0, numComments: hit.num_comments ?? 0 });
    }
  }
  console.error('\nPhase 3b done.');

  // Fetch uncached story authors
  const uncachedStoryAuthors = [...storyAuthorsSeen].filter(u => !getCached.get(u));
  if (uncachedStoryAuthors.length > 0) {
    console.error(`Phase 3b user fetch: ${uncachedStoryAuthors.length} uncached authors...`);
    let sf = 0;
    await withConcurrency(uncachedStoryAuthors, 20, async (username) => {
      const profile = await fetchUser(username);
      const now = Math.floor(Date.now() / 1000);
      upsert.run(username, profile?.created ?? null, profile?.karma ?? null, now);
      sf++;
      if (sf % 200 === 0) process.stderr.write(`\r  ${sf}/${uncachedStoryAuthors.length}`);
    });
    console.error('\n');
  }

  const topPosts = [...postsByVideo.entries()]
    .filter(([, data]) => data.count > 1)
    .sort((a, b) => b[1].count - a[1].count);

  // Phase 4: enrich both sets with YouTube metadata
  console.error('Phase 4: fetching YouTube metadata...');
  const allVideoIds = [...new Set([...topComments.map(([id]) => id), ...topPosts.map(([id]) => id)])];
  const ytMeta = new Map();
  for (let i = 0; i < allVideoIds.length; i += 50) {
    const batch = allVideoIds.slice(i, i + 50);
    const res = await fetch(`https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id=${batch.join(',')}&key=${YOUTUBE_API_KEY}`);
    const data = await res.json();
    for (const item of data.items ?? []) {
      ytMeta.set(item.id, {
        title: item.snippet.title,
        channel: item.snippet.channelTitle,
        views: parseInt(item.statistics.viewCount ?? '0', 10),
      });
    }
  }
  console.error('Phase 4 done.');

  const fs = require('fs');

  const commentsRanked = topComments
    .filter(([id]) => ytMeta.has(id))
    .map(([id, data]) => ({
      videoId: id,
      url: `https://www.youtube.com/watch?v=${id}`,
      hnRefs: data.count,
      title: ytMeta.get(id).title,
      channel: ytMeta.get(id).channel,
      views: ytMeta.get(id).views,
      storyTitles: [...data.storyTitles].slice(0, 3),
      storyIds: [...data.storyIds].slice(0, 3),
    }))
    .sort((a, b) => (a.views / a.hnRefs) - (b.views / b.hnRefs));

  const postsRanked = topPosts
    .filter(([id]) => ytMeta.has(id))
    .map(([id, data]) => ({
      videoId: id,
      url: `https://www.youtube.com/watch?v=${id}`,
      hnPosts: data.count,
      totalScore: data.totalScore,
      title: ytMeta.get(id).title,
      channel: ytMeta.get(id).channel,
      views: ytMeta.get(id).views,
      storyTitles: [...data.storyTitles].slice(0, 3),
      storyIds: [...data.storyIds].slice(0, 3),
    }))
    .sort((a, b) => (a.views / a.hnPosts) - (b.views / b.hnPosts));

  // Build data.json: per-video events for detail pages
  const allVideoIdsSet = new Set([...commentsRanked.map(v => v.videoId), ...postsRanked.map(v => v.videoId)]);
  const dataJson = {};
  for (const id of allVideoIdsSet) {
    const comments = (commentEvents.get(id) ?? []).filter(e => {
      // only qualified users, dedupe by storyId
      if (!qualifiedUsers.has(e.author)) return false;
      return true;
    });
    // dedupe comments by storyId (same logic as counting)
    const seenStoryForVideo = new Set();
    const dedupedComments = comments.filter(e => {
      const k = `${e.storyId}`;
      if (seenStoryForVideo.has(k)) return false;
      seenStoryForVideo.add(k);
      return true;
    });
    dataJson[id] = {
      comments: dedupedComments,
      posts: postEvents.get(id) ?? [],
    };
  }

  fs.writeFileSync('results.json', JSON.stringify(commentsRanked, null, 2));
  fs.writeFileSync('posts.json', JSON.stringify(postsRanked, null, 2));
  fs.writeFileSync('data.json', JSON.stringify(dataJson));
  console.error(`Done. ${commentsRanked.length} comment videos, ${postsRanked.length} post videos.`);
}

main().catch(err => { console.error(err); process.exit(1); });
