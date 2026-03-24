import puppeteer from '@cloudflare/puppeteer';

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        const targetUrl = url.searchParams.get('url');

        if (!targetUrl) {
            return new Response(JSON.stringify({ error: 'Missing url parameter' }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        let browser = null;

        try {
            // Launch browser with the binding
            browser = await puppeteer.launch(env.BROWSER);

            const page = await browser.newPage();

            // Set timeout and navigation options
            await page.setDefaultNavigationTimeout(30000);

            // Navigate to the target URL
            await page.goto(targetUrl, {
                waitUntil: 'networkidle0',
                timeout: 30000
            });

            // Get the HTML content
            const html = await page.content();

            return new Response(JSON.stringify({
                url: targetUrl,
                html: html,
                timestamp: new Date().toISOString()
            }), {
                headers: { 'Content-Type': 'application/json' }
            });

        } catch (error) {
            return new Response(JSON.stringify({
                error: error.message,
                url: targetUrl
            }), {
                status: 500,
                headers: { 'Content-Type': 'application/json' }
            });
        } finally {
            // CRITICAL: Always close browser to free up concurrency slot
            if (browser) {
                await browser.close();
            }
        }
    }
};
