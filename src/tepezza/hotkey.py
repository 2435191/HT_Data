from pynput.keyboard import Key, HotKey, Controller, Listener
from typing import Union, Optional
import logging
import threading
import clipboard
import time
import random

logger = logging.getLogger('tepezza hot key')

def _sleep(lower: int = 100, upper: int = 300) -> None:
    """Sleep for a uniform value betwee lower and upper (msec).
    :param lower:
    :type lower: float
    :param upper:
    :type upper: float
    """
    time.sleep(random.uniform(lower / 1000, upper / 1000))

def _tap(key: Union[str, Key], keyboard: Controller, lower: int = 50, upper: int = 100) -> None:
    keyboard.press(key)
    _sleep(lower, upper)
    keyboard.release(key)

def _type_zipcode(keyboard: Controller, zipcode: str) -> None:
    with keyboard.pressed(Key.cmd_l): # select all
        _tap('a', keyboard)
    _sleep()

    with keyboard.pressed(Key.cmd_l): # paste
        _tap('v', keyboard)
    _sleep(300, 600)

    _tap(Key.enter, keyboard)
    _sleep()

class TepezzaHotkey:
    def __init__(self):
        self._hotkey = HotKey(HotKey.parse('<cmd>+e'), self._on_activate)
        self._k = Controller()

        self.enabled = True
        self.zipcode = None

        self._l = Listener(
            on_press=self._for_canonical(self._hotkey.press),
            on_release=self._for_canonical(self._hotkey.release),
            daemon=True
        )

        
    def _for_canonical(self, func):
        # helper method
        return lambda k: func(self._l.canonical(k))

    def start(self, *args, **kwargs):
        self._l.start(*args, **kwargs)

    def _on_activate(self):
        if not self.enabled:
            logger.warning('Disabled TepezzaHotkey instance!')
            return
        if self.zipcode is None:
            logger.warning('Set zipcode first!')
            return

        _type_zipcode(self._k, self.zipcode)


if __name__ == '__main__':
    thk = TepezzaHotkey()
    thk.zipcode = '55902'
    thk.launch()