"""Check the image's Chromium policy and launcher without a root image build."""
import json
import pathlib
import unittest

SETUP = pathlib.Path(__file__).resolve().parents[1] / 'setup.sh'


class ChromiumSetupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script = SETUP.read_text()

    def test_managed_policy_disables_password_prompts(self):
        marker = "cat > /etc/chromium/policies/managed/swarmy.json <<'JSON'\n"
        policy = json.loads(self.script.split(marker, 1)[1].split('\nJSON\n', 1)[0])
        self.assertIs(policy['PasswordManagerEnabled'], False)
        self.assertIs(policy['PasswordLeakDetectionEnabled'], False)
        self.assertIs(policy['PromotionalTabsEnabled'], False)
        self.assertIn('chmod 644 /etc/chromium/policies/managed/swarmy.json', self.script)

    def test_launcher_hides_warning_without_dropping_required_flag(self):
        wrapper = self.script.split("cat > /usr/local/bin/chromium <<'SH'\n", 1)[1].split('\nSH\n', 1)[0]
        self.assertIn('chrome --no-sandbox --test-type', wrapper)
        self.assertIn('exec su -p -s /bin/sh agent', wrapper)


if __name__ == '__main__':
    unittest.main()
