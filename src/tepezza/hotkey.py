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

    with keyboard.pressed(Key.cmd_l): # paste
        _tap('v', keyboard)
    _sleep(300, 600)

    _tap(Key.enter, keyboard)
    _sleep()

    with keyboard.pressed(Key.cmd_l): # select all
        _tap('a', keyboard)
    _sleep()

    _tap(Key.backspace, keyboard)

class TepezzaHotkey:
    def __init__(self):
        self._hotkey = HotKey(HotKey.parse('<cmd>+e'), self._on_activate)
        self._k = Controller()

        self._enabled = True
        self._enabled_lock = threading.Lock()

        self._zipcode = None
        self._zipcode_lock = threading.Lock()

    def launch(self):
        def for_canonical(f):
            return lambda k: f(l.canonical(k))
        with Listener(
            on_press=for_canonical(self._hotkey.press),
            on_release=for_canonical(self._hotkey.release),
            daemon=True
        ) as l:
            l.join()

    def _on_activate(self):
        if not self.enabled:
            logger.warning('Disabled TepezzaHotkey instance!')
            return
        if self.zipcode is None:
            logger.warning('Set zipcode first!')
            return

        _type_zipcode(self._k, self.zipcode)



    @property
    def zipcode(self) -> Optional[str]:
        with self._zipcode_lock:
            return self._zipcode
    @zipcode.setter
    def zipcode(self, z: str):
        with self._zipcode_lock:
            self._zipcode = z

    @property
    def enabled(self) -> bool:
        with self._enabled_lock:
            return self._enabled
    @enabled.setter
    def enabled(self, e: bool):
        with self._enabled_lock:
            self._enabled = e


if __name__ == '__main__':
    thk = TepezzaHotkey()
    thk.zipcode = '55902'
    thk.launch()