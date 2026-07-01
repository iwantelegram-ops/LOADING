"""
Jalankan ini di Railway shell (atau lokal yang sama env-nya):
  python debug_check2.py
TIDAK connect ke Telegram, cuma cek struktur lib yang terinstall.
"""
import pyrogram
print("pyrogram.__version__ :", pyrogram.__version__)
try:
    print("pyrogram.__file__    :", pyrogram.__file__)
except Exception as e:
    print("no __file__:", e)

from pyrogram import raw
import inspect

for name in ["KeyboardButtonRequestPeer", "RequestPeerTypeChat", "ChatAdminRights", "ReplyKeyboardMarkup"]:
    try:
        cls = getattr(raw.types, name)
        sig = inspect.signature(cls.__init__)
        print(f"\n{name}:")
        print(" ", sig)
    except Exception as e:
        print(f"\n{name}: ERROR -> {e}")

print("\n--- Cek SendMessage flags wajib ---")
try:
    sig = inspect.signature(raw.functions.messages.SendMessage.__init__)
    print(sig)
except Exception as e:
    print("ERROR:", e)
