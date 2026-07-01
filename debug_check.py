"""
Jalankan script kecil ini di environment yang SAMA dengan bot Anda
(misal: railway run python debug_check.py, atau di shell lokal yang sudah
ada pyrogram terinstall). Ini HANYA mengecek struktur, TIDAK connect ke Telegram.
"""
import pyrogram
print("Pyrogram version:", pyrogram.__version__)

from pyrogram import raw
import inspect

print("\n=== ChatAdminRights fields ===")
print(inspect.signature(raw.types.ChatAdminRights.__init__))

print("\n=== KeyboardButtonRequestPeer fields ===")
print(inspect.signature(raw.types.KeyboardButtonRequestPeer.__init__))

print("\n=== RequestPeerTypeChat fields ===")
print(inspect.signature(raw.types.RequestPeerTypeChat.__init__))

print("\n=== ReplyKeyboardMarkup (raw) fields ===")
print(inspect.signature(raw.types.ReplyKeyboardMarkup.__init__))
