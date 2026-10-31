"""Check the shared desktop setup's display selection without a root image build."""
import pathlib
import subprocess
import tempfile
import unittest

SETUP = pathlib.Path(__file__).resolve().parents[1] / 'desktop-setup.sh'
NVIDIA_LIBRARIES = [(1, 'nvidia_drv.so'), (1, 'libglxserver_nvidia.so.550.127.05'),
                    (0, 'libGLX_nvidia.so.0')]


def block(script, path, end):
    return script.split("cat > %s <<'%s'\n" % (path, end), 1)[1].split('\n%s\n' % end, 1)[0]


class DesktopSetupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script = SETUP.read_text()

    def choose(self, devices, libraries):
        """Run the selector against a fake /dev and two fake library directories."""
        selector = block(self.script, '/usr/local/libexec/swarmy-display-select', 'SH')
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            dev = root / 'dev'
            libs = [root / 'lib', root / 'lib' / 'nvidia' / 'xorg']
            for path in [dev, *libs]:
                path.mkdir(parents=True, exist_ok=True)
            for name in devices:
                (dev / name).touch()
            for index, name in libraries:
                (libs[index] / name).touch()
            program = root / 'select'
            program.write_text(selector + '\n')
            result = subprocess.run(['sh', str(program), str(dev), *map(str, libs)],
                                    capture_output=True, text=True, check=True)
            return result.stdout

    def test_card_and_injected_libraries_select_nvidia(self):
        self.assertEqual(self.choose(['nvidia0', 'nvidiactl'], NVIDIA_LIBRARIES), 'nvidia\n')

    def test_no_card_selects_software(self):
        self.assertEqual(self.choose(['nvidiactl'], NVIDIA_LIBRARIES), 'software\n')

    def test_card_without_libraries_selects_software(self):
        self.assertEqual(self.choose(['nvidia0'], []), 'software\n')

    def test_card_with_partial_libraries_selects_software(self):
        for missing in NVIDIA_LIBRARIES:
            present = [library for library in NVIDIA_LIBRARIES if library != missing]
            self.assertEqual(self.choose(['nvidia0'], present), 'software\n', missing)

    def test_shell_programs_parse(self):
        for path, end in [('/usr/local/libexec/swarmy-display-select', 'SH'),
                          ('/usr/local/libexec/swarmy-init', 'SH'),
                          ('/usr/local/bin/chromium', 'SH')]:
            result = subprocess.run(['sh', '-n'], input=block(self.script, path, end) + '\n',
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, (path, result.stderr))

    def test_software_display_is_todays_xvfb(self):
        init = block(self.script, '/usr/local/libexec/swarmy-init', 'SH')
        self.assertIn('setpriv --reuid agent --regid agent --clear-groups Xvfb :99 '
                      '-screen 0 1280x800x24 -nolisten tcp &', init)
        self.assertIn('renderer=$(/usr/local/libexec/swarmy-display-select)', init)
        self.assertIn("printf '%s\\n' \"$renderer\" > /run/swarmy-display/renderer", init)

    def test_nvidia_display_has_a_virtual_screen_and_no_monitor(self):
        config = block(self.script, '/etc/X11/swarmy-nvidia.conf', 'CONF')
        for line in ['Driver "nvidia"', 'Option "AllowEmptyInitialConfiguration" "true"',
                     'Virtual 1280 800',
                     'Option "AutoAddDevices" "false"']:
            self.assertIn(line, config)
        # The L4's driver refuses UseDisplayDevice "None" with a virtual
        # screen: "UseDisplayDevice "None" is not supported with virtual display".
        self.assertNotIn('UseDisplayDevice', config)
        init = block(self.script, '/usr/local/libexec/swarmy-init', 'SH')
        self.assertIn('Xorg :99 -config /etc/X11/swarmy-nvidia.conf', init)

    def test_sandbox_environment_does_not_pin_vulkan_to_lavapipe(self):
        environment = block(self.script, '/etc/swarmy/environment', 'ENV')
        self.assertEqual(environment, 'DISPLAY=:99\nLIBGL_ALWAYS_SOFTWARE=1\nGALLIUM_DRIVER=llvmpipe')

    def test_image_installs_no_nvidia_package(self):
        self.assertNotRegex(self.script, r'apt-get install[^\n]*(\\\n[^\n]*)*nvidia')


if __name__ == '__main__':
    unittest.main()
