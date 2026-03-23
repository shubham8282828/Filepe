# config.py
# Yahan se poora bot apni settings padta hai

import os
from dotenv import load_dotenv

# .env file load karo (local development ke liye)
load_dotenv()

class Config:
    """
    Bot ki saari settings ek jagah.
    Render pe env variables set karte hain,
    local pe .env file use hoti hai.
    """
    
    # ── Telegram Settings ──────────────────────────────
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    BOT_USERNAME: str = os.getenv("BOT_USERNAME", "")
    
    # Admin IDs — comma separated string ko list mein convert karo
    # Example: "123,456" → [123, 456]
    ADMIN_IDS: list[int] = [
        int(x.strip()) 
        for x in os.getenv("ADMIN_IDS", "").split(",") 
        if x.strip().isdigit()
    ]
    
    # ── MongoDB Settings ───────────────────────────────
    MONGO_URI: str = os.getenv("MONGO_URI", "")
    DB_NAME: str = os.getenv("DB_NAME", "telegram_bot_db")
    
    # ── Pixeldrain Settings ────────────────────────────
    PIXELDRAIN_API_KEY: str = os.getenv("PIXELDRAIN_API_KEY", "")
    PIXELDRAIN_BASE_URL: str = "https://pixeldrain.com/api"
    
    # ── Server Settings ────────────────────────────────
    PORT: int = int(os.getenv("PORT", 8080))
    
    # ── Token System Settings ──────────────────────────
    TOKEN_VALIDITY_HOURS: int = 24  # Token 24 ghante valid rahega
    
    # ── Referral Settings ─────────────────────────────
    REFERRAL_REWARD_DAYS: int = 1   # Referral pe 1 din premium milega
    
    @classmethod
    def validate(cls):
        """
        Check karo ki important settings missing toh nahi hain.
        Bot start hone se pehle ye run hoga.
        """
        errors = []
        
        if not cls.BOT_TOKEN:
            errors.append("❌ BOT_TOKEN missing!")
        if not cls.MONGO_URI:
            errors.append("❌ MONGO_URI missing!")
        if not cls.ADMIN_IDS:
            errors.append("❌ ADMIN_IDS missing!")
            
        if errors:
            print("\n".join(errors))
            print("\n⚠️  .env file check karo ya Render env variables set karo")
            return False
        return True

# Global config object — poore project mein yahi use hoga
config = Config()
