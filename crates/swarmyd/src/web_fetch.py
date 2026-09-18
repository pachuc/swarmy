# Run in the sandbox so fetching uses the agent's network and certificate store.
from html.parser import HTMLParser
import json
import signal
import sys
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, build_opener

MAX_BYTES = 5 * 1024 * 1024
TIMEOUT_SECONDS = 30


def validate_url(url):
    if urlsplit(url).scheme.lower() not in ('http', 'https'):
        raise ValueError('web_fetch accepts only HTTP and HTTPS URLs')


class Redirects(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, url):
        validate_url(url)
        return super().redirect_request(request, fp, code, message, headers, url)


class PageText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self.hidden += 1
        if not self.hidden and tag in ('br', 'p', 'div', 'li', 'tr', 'h1', 'h2', 'h3', 'pre'):
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self.hidden = max(0, self.hidden - 1)
        if not self.hidden and tag in ('p', 'div', 'li', 'tr', 'h1', 'h2', 'h3', 'pre'):
            self.parts.append('\n')

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def expired(signum, frame):
    raise TimeoutError('web_fetch exceeded the 30 second timeout')


def fetch(url):
    validate_url(url)
    # Socket timeouts alone would allow a slow stream to run indefinitely.
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, TIMEOUT_SECONDS)
    try:
        with build_opener(Redirects()).open(url, timeout=TIMEOUT_SECONDS) as response:
            length = response.headers.get('Content-Length')
            if length is not None and int(length) > MAX_BYTES:
                raise ValueError('web_fetch response exceeds the 5 MiB size cap')
            body = response.read(MAX_BYTES + 1)
            if len(body) > MAX_BYTES:
                raise ValueError('web_fetch response exceeds the 5 MiB size cap')
            text = body.decode(response.headers.get_content_charset() or 'utf-8', errors='replace')
            content_type = response.headers.get_content_type()
            if content_type in ('text/html', 'application/xhtml+xml'):
                page = PageText()
                page.feed(text)
                page.close()
                text = '\n'.join(line.strip() for line in ''.join(page.parts).splitlines() if line.strip())
            return dict(url=response.url, content_type=content_type, output=text)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


if __name__ == '__main__':
    try:
        print(json.dumps(fetch(sys.argv[1])))
    except (OSError, ValueError, LookupError, URLError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
