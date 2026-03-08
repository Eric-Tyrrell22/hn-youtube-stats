const { test, expect } = require('@playwright/test');

const BASE = 'http://localhost:8080';
const COMMENT_VIDEO_ID = '5ESJH1NLMLs';
const POST_VIDEO_ID = 'eWMM4J_rfDg';

test.describe('Comments page (index.html)', () => {
  test('loads and shows rows', async ({ page }) => {
    await page.goto(`${BASE}/index.html`);
    await page.waitForSelector('.tabulator-row');
    const rows = page.locator('.tabulator-row');
    await expect(rows).not.toHaveCount(0);
  });

  test('row click navigates to video page', async ({ page }) => {
    await page.goto(`${BASE}/index.html`);
    await page.waitForSelector('.tabulator-row');
    await page.locator('.tabulator-row').first().click();
    await page.waitForURL(/video\.html\?v=/);
    expect(page.url()).toMatch(/video\.html\?v=/);
  });

  test('search filters rows', async ({ page }) => {
    await page.goto(`${BASE}/index.html`);
    await page.waitForSelector('.tabulator-row');
    await page.fill('#search', 'zzznomatch999');
    await page.waitForTimeout(300);
    const rows = await page.locator('.tabulator-row').count();
    expect(rows).toBe(0);
  });

  test('views range filter works', async ({ page }) => {
    await page.goto(`${BASE}/index.html`);
    await page.waitForSelector('.tabulator-row');
    // Move max handle to step 1 (1K) via noUiSlider API
    await page.evaluate(() => {
      document.getElementById('views-slider').noUiSlider.set([0, 1]);
    });
    await page.waitForTimeout(300);
    const label = await page.textContent('#views-val');
    expect(label).not.toBe('Any');
  });
});

test.describe('Posts page (posts.html)', () => {
  test('loads and shows rows', async ({ page }) => {
    await page.goto(`${BASE}/posts.html`);
    await page.waitForSelector('.tabulator-row');
    const rows = page.locator('.tabulator-row');
    await expect(rows).not.toHaveCount(0);
  });

  test('row click navigates to video page', async ({ page }) => {
    await page.goto(`${BASE}/posts.html`);
    await page.waitForSelector('.tabulator-row');
    await page.locator('.tabulator-row').first().click();
    await page.waitForURL(/video\.html\?v=/);
    expect(page.url()).toMatch(/video\.html\?v=/);
  });

  test('search filters rows', async ({ page }) => {
    await page.goto(`${BASE}/posts.html`);
    await page.waitForSelector('.tabulator-row');
    await page.fill('#search', 'zzznomatch999');
    await page.waitForTimeout(300);
    const rows = await page.locator('.tabulator-row').count();
    expect(rows).toBe(0);
  });
});

test.describe('Video page (video.html)', () => {
  test('loads with valid video ID', async ({ page }) => {
    await page.goto(`${BASE}/video.html?v=${COMMENT_VIDEO_ID}`);
    await expect(page.locator('h1')).not.toHaveText('');
    await expect(page.locator('.stat')).not.toHaveCount(0);
  });

  test('shows not-found for missing video ID', async ({ page }) => {
    await page.goto(`${BASE}/video.html?v=DOESNOTEXIST`);
    await expect(page.locator('#not-found')).toBeVisible();
  });

  test('shows not-found when no ID param', async ({ page }) => {
    await page.goto(`${BASE}/video.html`);
    await expect(page.locator('#not-found')).toBeVisible();
  });

  test('renders chart canvas', async ({ page }) => {
    await page.goto(`${BASE}/video.html?v=${COMMENT_VIDEO_ID}`);
    await page.waitForSelector('#chart');
    await expect(page.locator('#chart')).toBeVisible();
  });

  test('renders tables', async ({ page }) => {
    await page.goto(`${BASE}/video.html?v=${COMMENT_VIDEO_ID}`);
    await page.waitForSelector('.tabulator-row');
    await expect(page.locator('.tabulator')).not.toHaveCount(0);
  });

  test('nav links work', async ({ page }) => {
    await page.goto(`${BASE}/video.html?v=${COMMENT_VIDEO_ID}`);
    await page.click('a.nav-link[href="index.html"]');
    await expect(page).toHaveURL(/index\.html/);
  });
});
