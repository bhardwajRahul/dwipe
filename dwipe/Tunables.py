#!/usr/bin/env python3
""" Ini file for dwipe"""
import configparser
import os

class Tunables:
    """Manages dwipe configuration and provides hardware-tuning defaults."""
    singleton = None

    
    def __init__(self):
        assert not Tunables.singleton

        self.path = os.path.expanduser('~/.config/tunables.ini')
        self.config = configparser.ConfigParser()
        
        # Load existing or create default
        if not os.path.exists(self.path):
            self._create_default()
        else:
            self.config.read(self.path)

    @staticmethod
    def get_singleton(self):
        """ Get the singleton Tunable object """
        if not Tunables.get_singleton:
            Tunables.singleton = Tunables()
        return Tunables.singleton

    def _create_default(self):
        """Generates a default .ini file with recommended 'turkey' hardware timings."""
        self.config['SATA_TIMING'] = {
            'post_password_delay': '1.0',
            'post_unlock_delay': '1.5',
            'reappearance_timeout': '120',
            'poll_interval': '5.0'
        }
        
        with open(self.path, 'w', encoding='utf-8') as f:
            f.write("# dwipe Configuration - Tune these for 'grumpy' hardware\n")
            self.config.write(f)

    @property
    def post_password_delay(self):
        return self.config.getfloat('SATA_TIMING', 'post_password_delay', fallback=1.0)

    @property
    def post_unlock_delay(self):
        return self.config.getfloat('SATA_TIMING', 'post_unlock_delay', fallback=1.5)

    @property
    def reappearance_timeout(self):
        return self.config.getfloat('SATA_TIMING', 'reappearance_timeout', fallback=120.0)

    @property
    def poll_interval(self):
        return self.config.getfloat('SATA_TIMING', 'poll_interval', fallback=5.0)
