import json
import base64
import uuid
import aiohttp
import time
import datetime
import discord
import asyncio
import re
import secrets
import string
import os
import functools
import io
import calendar
import pytz
import unicodedata
import random
from urllib.parse import quote
from dateutil.relativedelta import relativedelta
from aiohttp_socks import ProxyConnector, ProxyConnectionError
from itertools import cycle
from dotenv import load_dotenv
from functools import wraps
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Literal
from discord.ext import commands, tasks
from discord import app_commands, Embed, ButtonStyle, Color, TextStyle, SelectOption
from discord.ui import View, Button, Modal, TextInput, Select
from uuid import uuid4
load_dotenv()
intents = discord.Intents.all()
bot = commands.Bot(command_prefix="/", intents=intents)

AUTH_URL = "https://discord.com/oauth2/authorize?client_id=1367892670390730812"
FOUNDER_ROLE = 1308322106005782549
BOT_STOPPED = False
HIDE_EMOJI = "<:Hide:1367929687338123497>"
CALENDAR_EMOJI = "<:Calendar:1367929720439832596>"
CHECK_EMOJI = "<:Check:1367939014732283914>"
FAIL_EMOJI = "<:Fail:1367939006163324928>"
DIAMOND_TROPHY_EMOJI = "<:DiamondTrophy:1367929741763543130>"
DUO_MAD_EMOJI = "<:DuoMad:1367929136525348864>"
MAX_EMOJI = "<:Max:1367929152493064223>"
DUOLINGO_TRAINING_EMOJI = "<:DuolingoTraining:1367946724147990558>"
EYES_EMOJI = "<:Eyes:1367929752945561660>" 
TRASH_EMOJI = "<:Trash:1367929731495759882>"
GEM_EMOJI = "<:Gem:1367929660373078076>" 
GLOBE_DUOLINGO_EMOJI = "<:GlobeDuolingo:1367929764677029970>" 
HOME_EMOJI = "<:Home:1367939034311295006>" 
XP_EMOJI = "<:XP:1367938999565680690>"
STREAK_EMOJI = "<:Streak:1367929779285790831>" 
SUPER_EMOJI = "<:Super:1367929702479560764>" 
NERD_EMOJI = "<:Nerd:1367929711447244861>" 
QUEST_EMOJI = "<:Quest:1367928949740540016>" 
SHOP = "<#1308337662519803904>"
TOKEN = os.getenv('DISCORD_TOKEN')
MONGODB_URI = os.getenv('MONGODB_URI')
YEUMONEY_TOKEN = os.getenv('YEUMONEY_TOKEN')
LINK4M_TOKEN = os.getenv('LINK4M_TOKEN')
LINK2M_TOKEN = os.getenv('LINK2M_TOKEN')
CLKSH_TOKEN = os.getenv('CLKSH_TOKEN')
SHRINKEARN_TOKEN = os.getenv('SHRINKEARN_TOKEN')
SEOTRIEUVIEW_TOKEN = os.getenv('SEOTRIEUVIEW_TOKEN')
VERSION = "3.9.8"
CODENAME = "monoFlow Plus"
TOTAL_SERVERS_CHANNEL_ID = 1329827047505399888
LOG_ID = 1327639092435095573
ERROR_ID = 1308338102158495824
SUCCESS_ID = 1316357045905264660
SERVER = 1308317770017935380
PRE_ROLE = 1308322457584795678
FREE_ROLE = 1343214504707751936
ULTRA_ROLE = 1368635360288178276
MOD_ROLE = 1308376130453245982
USER_COUNT = 1308357361227792457
XP = 1308351056606138428
STREAK_BUFF = 1308351371505958913
GEM = 1308351421413982219
STREAK_SAVER = 1308351483980283975
LEAGUE = 1308351543644389397
LEAGUE_FARMERS = 1315952244541362206
DUO_ACCOUNT = 1308351593808396288
FOLLOW_ACCOUNT = 1352800705144033362
STATUS = 1308351660992757760
GIFT_CHANNEL = 1311590373386358784
QUEST_SAVER = 1321915274634723378
LOOP_COOLDOWN = 86400
CMD_COOLDOWN = 600
LOOPS = 10000
NAME = "www․DuoXPy․site"
FREE_ACCOUNT_LIMIT = 2
PREMIUM_ACCOUNT_LIMIT = 20
USE_PROXY_INDEX = 0

class Task:
    def __init__(self, discord_id, duolingo_id, task_type, amount, message_id, position=None):
        self.task_id = str(uuid4())
        self.discord_id = discord_id
        self.duolingo_id = duolingo_id
        self.task_type = task_type
        self.amount = amount
        self.position = position
        self.message_id = message_id
        self.start_time = datetime.now(pytz.UTC)
        self.end_time = None
        self.status = "running"
        self.progress = 0
        self.total = 0
        self.estimated_time_left = None
        self.gained = 0
    def to_dict(self):
        return self.__dict__

TASKS = {}

def check_bot_running():
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            if BOT_STOPPED:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Smart Maintenance",
                    description=f"{FAIL_EMOJI} The bot is currently under maintenance by itself to fix some issues. Please try again later.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name, 
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return
            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

def is_in_guild():
    async def predicate(interaction: discord.Interaction):
        specific_guild = bot.get_guild(SERVER)
        if specific_guild:
            member = specific_guild.get_member(interaction.user.id)
            if member is None:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Access Denied",
                    description=f"{FAIL_EMOJI} This bot is exclusively available to members of the official [server](https://discord.gg/KBzNXRZRdz).",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.response.send_message(embed=embed, ephemeral=True)
                return False
        return True

    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            if await predicate(interaction):
                return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

def is_owner():
    async def predicate(interaction: discord.Interaction):
        guild = bot.get_guild(SERVER)
        member = guild.get_member(interaction.user.id)
        if member is None or not any(role.id == FOUNDER_ROLE for role in member.roles):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Access Denied",
                description=f"{FAIL_EMOJI} This command is only available to users with the specified role.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return False
        return True

    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            if await predicate(interaction):
                return await func(interaction, *args, **kwargs)
        return wrapper

    return decorator

def is_premium():
    async def predicate(interaction: discord.Interaction):
        if not await is_premium_user(interaction.user):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Access Denied",
                description=f"{FAIL_EMOJI} This command is only available to DuoXPy Premium users.\n{GEM_EMOJI} You can purchase our premium plan at {SHOP}\n{QUEST_EMOJI} Or using `/free` to get a free premium for 1 day.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return False
        return True

    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            if await predicate(interaction):
                return await func(interaction, *args, **kwargs)
        return wrapper

    return decorator

def is_paid_premium():
    async def predicate(interaction: discord.Interaction):
        if await is_premium_free_user(interaction.user):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Access Denied",
                description=f"{FAIL_EMOJI} This command is only available to DuoXPy Paid Premium users.\n{GEM_EMOJI} You can purchase our premium plan at {SHOP}.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return False
        return True

    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            if await predicate(interaction):
                return await func(interaction, *args, **kwargs)
        return wrapper

    return decorator
    
def custom_cooldown(rate: int, per: float):
    def decorator(func):
        command_name = func.__name__

        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            log_channel = interaction.client.get_channel(LOG_ID)
            channel_info = (
                f"{CALENDAR_EMOJI} Channel: **{interaction.channel.name}**"
                if isinstance(interaction.channel, discord.TextChannel)
                else f"{CALENDAR_EMOJI} Channel: **DM**"
            )
            log_embed = Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Usage Log",
                description=(
                    f"{DUOLINGO_TRAINING_EMOJI} Command: **{command_name}**\n"
                    f"{NERD_EMOJI} User: {interaction.user.mention}\n"
                    f"{channel_info}\n"
                    f"{CHECK_EMOJI} Server: **{interaction.guild.name if interaction.guild else 'DM'}**"
                ),
                color=Color.teal(),
                timestamp=datetime.now()
            )
            log_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await log_channel.send(embed=log_embed)

            if await is_premium_user(interaction.user) and not await is_premium_free_user(interaction.user):
                return await func(interaction, *args, **kwargs)

            current_time = time.time()
            user_id = str(interaction.user.id)

            cooldown_data = await db.cooldowns.find_one({
                "_id": f"{user_id}:{command_name}"
            })

            if not cooldown_data:
                cooldown_data = {
                    "_id": f"{user_id}:{command_name}",
                    "uses_left": rate,
                    "last_used": current_time,
                    "rate": rate,
                    "per": per
                }
                await db.cooldowns.insert_one(cooldown_data)
            else:
                if cooldown_data["rate"] != rate or cooldown_data["per"] != per:
                    cooldown_data["rate"] = rate
                    cooldown_data["per"] = per
                    await db.cooldowns.update_one(
                        {"_id": f"{user_id}:{command_name}"},
                        {"$set": {"rate": rate, "per": per}}
                    )

            time_since_last = current_time - cooldown_data["last_used"]
            if time_since_last >= cooldown_data["per"]:
                cooldown_data["uses_left"] = rate
                cooldown_data["last_used"] = current_time
                await db.cooldowns.update_one(
                    {"_id": f"{user_id}:{command_name}"},
                    {"$set": cooldown_data}
                )
            if cooldown_data["uses_left"] > 0:
                cooldown_data["uses_left"] -= 1
                await db.cooldowns.update_one(
                    {"_id": f"{user_id}:{command_name}"},
                    {"$set": {
                        "uses_left": cooldown_data["uses_left"],
                        "last_used": current_time
                    }}
                )
                return await func(interaction, *args, **kwargs)
            time_until_next = cooldown_data["last_used"] + cooldown_data["per"] - current_time
            next_hours = int(time_until_next // 3600)
            next_minutes = int((time_until_next % 3600) // 60)
            next_seconds = int(time_until_next % 60)

            next_time_str = f"{next_hours}h {next_minutes}m {next_seconds}s".strip()

            cooldown_period_str = f"{int(per // 3600)}h {int((per % 3600) // 60)}m {int(per % 60)}s".strip()
            embed = Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Cooldown",
                description=(
                    f"{FAIL_EMOJI} You can use this command in {next_time_str}.\n"
                    f"{DUOLINGO_TRAINING_EMOJI} Uses left: **{cooldown_data['uses_left']}/{rate}**\n"
                    f"{CALENDAR_EMOJI} Cooldown period: **{cooldown_period_str}**"
                ),
                color=Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return

        return wrapper
    return decorator
   
class DatabaseManager:
    def __init__(self, uri: str) -> None:
        self.client = AsyncIOMotorClient(uri)
        self.database: AsyncIOMotorDatabase = self.client["duoxpy"]
        
        self.discord: AsyncIOMotorCollection = self.database["discord"]
        self.duolingo: AsyncIOMotorCollection = self.database["duolingo"]
        self.keys: AsyncIOMotorCollection = self.database["keys"]
        self.proxies: AsyncIOMotorCollection = self.database["proxies"]
        self.superlinks: AsyncIOMotorCollection = self.database["superlinks"]
        self.loop_usage: AsyncIOMotorCollection = self.database["loop_usage"]
        self.cooldowns: AsyncIOMotorCollection = self.database["cooldowns"]
        self.follow: AsyncIOMotorCollection = self.database["follow"]
        self.report: AsyncIOMotorCollection = self.database["report"]
        self.victim: AsyncIOMotorCollection = self.database["victim"]

    async def login_dis(self, discord_id: int) -> None:
        await self.discord.find_one_and_update(
            {"_id": discord_id},
            {"$set": {
                "selected": None,
                "streaksaver": False,
                "questsaver": False,
                "hide": False,
                "autoleague": {
                    "active": False,
                    "target": None,
                    "autoblock": False
                },
                "premium": {
                    "active": False,
                    "type": None,
                    "duration": None,
                    "start": None,
                    "end": None
                }
            }},
            upsert=True
        )

    async def login(self, discord_id: int, duolingo_id: int, jwt_token: str, duolingo_username: str, timezone: str) -> None:    
        await self.duolingo.find_one_and_update(
            {"_id": duolingo_id},
            {"$set": {
                "username": duolingo_username,
                "discord_id": discord_id,
                "jwt_token": jwt_token,
                "timezone": timezone,
                "paused": False
            }},
            upsert=True,
        )
        await self.discord.find_one_and_update(
            {"_id": discord_id},
            {"$set": {
                "selected": duolingo_id
            }},
            upsert=True
        )

    async def list_profiles(self, discord_id: int) -> List[Dict[str, Any]]:
        profile_list = self.duolingo.find({"discord_id": discord_id})
        return [profile async for profile in profile_list]
    
    async def get_selected_profile(self, discord_id: int) -> Optional[Dict[str, Any]]:
        discord_account = await self.discord.find_one({"_id": discord_id})
        if not discord_account:
            return None
            
        duolingo_id = discord_account.get("selected")
        if not duolingo_id:
            return None
            
        return await self.duolingo.find_one({"_id": duolingo_id})
        
    async def select_profile(self, discord_id: int, duolingo_id: int) -> None:
        await self.discord.find_one_and_update(
            {"_id": discord_id},
            {"$set": {"selected": duolingo_id}},
            upsert=True
        )

    async def load_keys(self) -> Dict[str, Any]:
        cursor = self.keys.find({})
        keys_dict = {}
        async for doc in cursor:
            key_id = doc.pop("_id")
            keys_dict[key_id] = doc
        return keys_dict

    async def load_proxies(self) -> Dict[str, bool]:
        cursor = self.proxies.find({})
        return {doc["_id"]: True async for doc in cursor}

    async def save_proxies(self, proxies: Dict[str, bool]) -> None:
        await self.proxies.delete_many({})
        if proxies:
            await self.proxies.insert_many(
                [{"_id": proxy} for proxy in proxies.keys()]
            )

    async def load_superlinks(self) -> Dict[str, bool]:
        cursor = self.superlinks.find({})
        return {doc["_id"]: True async for doc in cursor}

    async def save_superlinks(self, superlinks: Dict[str, bool]) -> None:
        await self.superlinks.delete_many({})
        if superlinks:
            await self.superlinks.insert_many(
                [{"_id": link} for link in superlinks.keys()]
            )

db = DatabaseManager(MONGODB_URI)

class LinkView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.jwt_guide_shown = False

    async def on_timeout(self):
        if not self.is_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None)

    async def setup_buttons(self):
        discord_account = await db.discord.find_one({"_id": self.user_id})
        
        if not discord_account:
            link_discord_button = discord.ui.Button(
                label="Link Discord Account", 
                style=ButtonStyle.secondary,
                emoji=QUEST_EMOJI
            )
            
            async def link_discord_callback(interaction: discord.Interaction):
                await db.login_dis(self.user_id)
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Account Linked",
                    description=f"{CHECK_EMOJI} Discord account linked successfully! Please use `/account` again to link your Duolingo account.",
                    color=discord.Color.green()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.response.edit_message(embed=embed, view=None)
                self.is_completed = True
                
            link_discord_button.callback = link_discord_callback
            self.add_item(link_discord_button)
            
        else:
            email_button = discord.ui.Button(
                label="Link with Email/Username",
                style=ButtonStyle.secondary,
                emoji=QUEST_EMOJI
            )
            email_button.callback = self.email_button_callback
            self.add_item(email_button)

            jwt_button = discord.ui.Button(
                label="Link with JWT Token",
                style=ButtonStyle.secondary,
                emoji=DUO_MAD_EMOJI
            )
            jwt_button.callback = self.jwt_button_callback
            self.add_item(jwt_button)

    async def email_button_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(EmailLoginModal(self))

    async def jwt_button_callback(self, interaction: discord.Interaction):
        if not self.jwt_guide_shown:
            embed = Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: JWT Token Guide",
                description=(
                    "### To Get Your JWT Token:\n"
                    "- **Desktop**: Go to https://duolingo.com while logged in, open Developer Tools (**Ctrl + Shift + i**), and navigate to the Console tab.\n"
                    "- Paste the following code in the Console to retrieve your JWT token:\n"
                    "```js\n"
                    "document.cookie.match(new RegExp('(^| )jwt_token=([^;]+)'))[0].slice(11)\n"
                    "```\n"
                    "- **Mobile**: Use [Web Inspector for Apple](https://apps.apple.com/us/app/web-inspector/id1584825745) or [Kiwi Browser for Android](https://play.google.com/store/apps/details?id=com.kiwibrowser.browser&hl=en&pli=1).\n\n"
                    "Click **Continue** below to enter your token."
                ),
                color=Color.teal()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            
            continue_view = discord.ui.View(timeout=60)
            continue_button = discord.ui.Button(label="Continue", style=ButtonStyle.secondary, emoji=CHECK_EMOJI)
            
            async def continue_callback(i: discord.Interaction):
                await i.response.send_modal(JWTModal(self))
            
            continue_button.callback = continue_callback
            continue_view.add_item(continue_button)
            
            await interaction.response.edit_message(embed=embed, view=continue_view)
            continue_view.message = await interaction.original_response()
            self.jwt_guide_shown = True
        else:
            await interaction.response.send_modal(JWTModal(self))

class EmailLoginModal(Modal, title="Login Form"):
    def __init__(self, link_view: LinkView):
        super().__init__()
        self.link_view = link_view
        
    email = TextInput(
        label="Email/Username",
        style=TextStyle.short,
        placeholder="Enter your Duolingo email or username",
        required=True
    )
    password = TextInput(
        label="Password",
        style=TextStyle.short,
        placeholder="Enter your password",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        user = interaction.user
        misc = Miscellaneous()
        wait_embed = Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Logging in to your account...",
            color=Color.teal()
        )
        wait_embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
        await interaction.edit_original_response(embed=wait_embed, view=None)

        distinct_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
        
        try:
            async with await get_session(direct=True) as session:
                login_data = {
                    "distinctId": distinct_id,
                    "identifier": self.email.value,
                    "password": self.password.value
                }
                
                async with session.post(
                    "https://www.duolingo.com/2017-06-30/login?fields=",
                    json=login_data,
                    headers={
                        "Host": "www.duolingo.com",
                        "User-Agent": misc.randomize_mobile_user_agent(),
                        "Accept": "application/json",
                        "X-Amzn-Trace-Id": "User=0",
                        "Accept-Encoding": "gzip, deflate, br"
                    }
                ) as response:
                    if response.status != 200:
                        self.link_view.is_completed = True
                        self.link_view.stop()
                        embed = Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Link Failed",
                            description=f"{FAIL_EMOJI} Invalid username or password. Please try again.",
                            color=Color.red()
                        )
                        embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    jwt_token = response.headers.get('jwt')
                    if not jwt_token:
                        self.link_view.is_completed = True
                        self.link_view.stop()
                        embed = Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Link Failed",
                            description=f"{FAIL_EMOJI} Failed to get authentication token. Please try again.",
                            color=Color.red()
                        )
                        embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    await process_jwt_token(interaction, jwt_token, user)
                    self.link_view.is_completed = True
                    self.link_view.stop()

        except Exception as e:
            self.link_view.is_completed = True
            self.link_view.stop()
            embed = Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred during link. Please try again later.",
                color=Color.red()
            )
            embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
            await interaction.edit_original_response(embed=embed, view=None)

class JWTModal(Modal, title="Link Account Form"):
    def __init__(self, link_view: LinkView):
        super().__init__()
        self.link_view = link_view
        
    jwt_token_input = TextInput(
        label="JWT Token",
        style=TextStyle.short,
        placeholder="Enter your JWT token",
        required=True,
        min_length=10,
        max_length=2000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        user = interaction.user
        
        wait_embed = Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your JWT token...",
            color=Color.teal()
        )
        wait_embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
        await interaction.edit_original_response(embed=wait_embed, view=None)

        jwt_token = self.jwt_token_input.value.strip().replace(" ", "").replace("'", "")
        await process_jwt_token(interaction, jwt_token, user)
        self.link_view.is_completed = True
        self.link_view.stop()

async def process_jwt_token(interaction: discord.Interaction, jwt_token: str, user):
    user_id = interaction.user.id

    if not re.match(r'^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+$', jwt_token):
        embed = Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Invalid Token",
            description=f"{FAIL_EMOJI} The JWT token is invalid. Please ensure it's correctly formatted and try again.",
            color=Color.red()
        )
        embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
        await interaction.edit_original_response(embed=embed, view=None)
        return

    duo_id = await extract_duolingo_user_id(jwt_token)
    if not duo_id:
        embed = Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Error",
            description=f"{FAIL_EMOJI} Failed to extract Duolingo user ID from JWT token. Please ensure your token is valid.",
            color=Color.red()
        )
        embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
        await interaction.edit_original_response(embed=embed, view=None)
        return
            
    existing_duo_profile = await db.duolingo.find_one({"_id": duo_id})
    if existing_duo_profile:
        embed = Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Account Already Linked",
            description=f"{FAIL_EMOJI} This Duolingo account is already linked to another Discord account. Please unlink it first.",
            color=Color.red()
        )
        embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
        await interaction.edit_original_response(embed=embed, view=None)
        return

    async with await get_session(direct=True) as session:
        headers = await getheaders(jwt_token, duo_id)
        url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}"
        
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                embed = Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Profile Retrieval Failed",
                    description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please check your token or try again later.",
                    color=Color.red()
                )
                embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
                await interaction.edit_original_response(embed=embed)
                return
                
            profile_data = await response.json()
            
            if profile_data:
                display_name = profile_data.get('name', '').lower()
                normalized_name = unicodedata.normalize('NFKD', display_name)
                normalized_name = ''.join(c for c in normalized_name if c.isalnum()).lower()
                prohibited_patterns = [
                    r'.*a.*u.*t.*o.*d.*u.*o.*',
                    r'.*d.*u.*o.*s.*m.*',
                    r'.*c.*l.*i.*c.*k.*',
                    r'.*s.*u.*p.*e.*r.*d.*u.*o.*l.*i.*n.*g.*o.*',
                    r'.*f.*a.*m.*i.*l.*y.*',
                    r'.*s.*u.*p.*e.*r.*d.*u.*o.*',
                ]
                
                is_prohibited = any(re.match(pattern, normalized_name) for pattern in prohibited_patterns)
                
                if is_prohibited:
                    try:
                        guild = bot.get_guild(SERVER)
                        member = guild.get_member(user_id)
                        if member:
                            await member.ban(reason=f"Using prohibited display name: {display_name}")
                            print(f"[LOG] Banned user {user_id} for using prohibited display name: {display_name}")
                    except Exception as e:
                        print(f"[LOG] Failed to ban user {user_id}: {e}")
                        
                    embed = Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Prohibited Name",
                        description=f"{FAIL_EMOJI} This account's display name contains prohibited terms.",
                        color=Color.red()
                    )
                    embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duolingo_username = profile_data.get("username", "Unknown")
                profile_link = f"https://www.duolingo.com/profile/{duolingo_username}"
                timezone = profile_data.get("timezone", "Asia/Saigon")
                is_premium = await is_premium_user(interaction.user)

                if not is_premium:
                    payload = {
                        "name": NAME,
                        "username": duolingo_username,
                        "email": profile_data.get("email", ""),
                        "signal": None
                    }
                    patch_url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}"
                    async with session.patch(patch_url, headers=headers, json=payload) as patch_response:
                        if patch_response.status != 200:
                            print(f"[LOG] Failed to update display name for free user")

                await db.login(user_id, duo_id, jwt_token, duolingo_username, timezone)
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Linked",
                    description=f"{CHECK_EMOJI} Successfully linked to profile [**{duolingo_username}**]({profile_link}).",
                    color=Color.green()
                )
                embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
                await interaction.edit_original_response(embed=embed)
            else:
                embed = Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Profile Retrieval Failed", 
                    description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please check your token or try again later.",
                    color=Color.red()
                )
                embed.set_author(name=user.display_name, icon_url=user.avatar.url if user.avatar else user.display_avatar.url)
                await interaction.edit_original_response(embed=embed)

async def fetch_username_from_id(duo_id, session):
    url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}?fields=username"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('username')
    return None

async def check_account_limit(user_id, interaction):
    profiles = await db.list_profiles(user_id)
    account_count = len(profiles)
    is_premium = await is_premium_user(interaction.user)
    is_free = await is_premium_free_user(interaction.user)
    limit = PREMIUM_ACCOUNT_LIMIT if is_premium else FREE_ACCOUNT_LIMIT if is_free else FREE_ACCOUNT_LIMIT
    
    if account_count >= limit:
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Account Limit Reached", 
            description=f"{FAIL_EMOJI} You have reached the maximum number of **{limit}** accounts you can link.",
            color=discord.Color.red()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)
        return False
    return True
               
async def check_proxy_health(proxy):
    try:
        connector = ProxyConnector.from_url(proxy, force_close=True)
        timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get("https://www.duolingo.com") as response:
                return response.status == 200
    except Exception as e:
        print(f"[LOG] Proxy health check failed for {proxy}: {str(e)}")
        return False

async def get_session(slot: Optional[int] = None, direct: bool = False):
    global BOT_STOPPED, USE_PROXY_INDEX
    if direct:
        connector = aiohttp.TCPConnector(force_close=True)
        timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
        return aiohttp.ClientSession(connector=connector, timeout=timeout)

    proxies = await db.load_proxies()
    proxy_list = list(proxies.keys())
    
    if slot is not None and proxy_list:
        for i in range(len(proxy_list)):
            adjusted_slot = (slot + i) % len(proxy_list)
            proxy = proxy_list[adjusted_slot]
            connector = ProxyConnector.from_url(proxy, force_close=True)
            timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
            session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            
            try:
                async with session.get("https://www.duolingo.com") as response:
                    if response.status == 200:
                        BOT_STOPPED = False
                        return session
            except:
                await session.close()
                continue
        
        connector = aiohttp.TCPConnector(force_close=True)
        timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
        session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        try:
            async with session.get("https://www.duolingo.com") as response:
                if response.status == 200:
                    BOT_STOPPED = False
                    return session
                await session.close()
        except:
            await session.close()
            
        BOT_STOPPED = True
        connector = aiohttp.TCPConnector(force_close=True)
        timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
        return aiohttp.ClientSession(connector=connector, timeout=timeout)

    if not proxy_list:
        connector = aiohttp.TCPConnector(force_close=True)
        timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
        session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        try:
            async with session.get("https://www.duolingo.com") as response:
                if response.status == 200:
                    BOT_STOPPED = False
                    return session
                await session.close()
        except:
            await session.close()
            
        BOT_STOPPED = True
        connector = aiohttp.TCPConnector(force_close=True)
        timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
        return aiohttp.ClientSession(connector=connector, timeout=timeout)

    current_proxy_index = USE_PROXY_INDEX % len(proxy_list)
    proxy = proxy_list[current_proxy_index]
    connector = ProxyConnector.from_url(proxy, force_close=True)
    timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
    session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    
    try:
        async with session.get("https://www.duolingo.com") as response:
            if response.status == 200:
                BOT_STOPPED = False
                USE_PROXY_INDEX = (USE_PROXY_INDEX + 1) % len(proxy_list)
                return session
        await session.close()
    except:
        await session.close()

    for i in range(len(proxy_list) - 1):
        current_proxy_index = (current_proxy_index + 1) % len(proxy_list)
        proxy = proxy_list[current_proxy_index]
        
        connector = ProxyConnector.from_url(proxy, force_close=True)
        timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
        session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        
        try:
            async with session.get("https://www.duolingo.com") as response:
                if response.status == 200:
                    BOT_STOPPED = False
                    USE_PROXY_INDEX = (current_proxy_index + 1) % len(proxy_list)
                    return session
            await session.close()
        except:
            await session.close()
            continue

    connector = aiohttp.TCPConnector(force_close=True)
    timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
    session = aiohttp.ClientSession(connector=connector, timeout=timeout)
    
    try:
        async with session.get("https://www.duolingo.com") as response:
            if response.status == 200:
                BOT_STOPPED = False
                return session
            await session.close()
    except:
        await session.close()
        
    BOT_STOPPED = True
    connector = aiohttp.TCPConnector(force_close=True)
    timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
    return aiohttp.ClientSession(connector=connector, timeout=timeout)

async def check_duolingo_access():
    global BOT_STOPPED
    timeout = aiohttp.ClientTimeout(total=600, connect=600, sock_read=600, sock_connect=600)
    connector = aiohttp.TCPConnector(force_close=True)
    
    direct_failed = False
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get("https://www.duolingo.com/") as response:
                if response.status != 200:
                    direct_failed = True
    except:
        direct_failed = True
        
    proxy_list = await db.load_proxies()
    all_proxies_failed = True
    
    if proxy_list:
        for proxy in proxy_list:
            try:
                connector = ProxyConnector.from_url(proxy, force_close=True)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout, force_close=True) as proxy_session:
                    async with proxy_session.get("https://www.duolingo.com/") as response:
                        if response.status == 200:
                            all_proxies_failed = False
                            break
            except:
                continue

    if direct_failed and all_proxies_failed:
        BOT_STOPPED = True
        print("[LOG] All connections failed. Setting BOT_STOPPED to True...")
    else:
        BOT_STOPPED = False
        print("[LOG] At least one connection successful. Setting BOT_STOPPED to False...")

async def extract_code(urlorcode: str) -> str:
    if 'family-plan' in urlorcode:
        return urlorcode.split('/')[-1]
    return urlorcode

async def check_link_validity(link, session=None):
    try:
        if "/family-plan/" in link:
            invite_code = link.split("/family-plan/")[1]
        else:
            return False
        api_url = f"https://www.duolingo.com/2017-06-30/family-plan/invite/{invite_code}"
        async with session.get(api_url) as response:
            response.raise_for_status()
            data = await response.json()
            return data.get("isValid", False)
    except Exception:
        ...
    return False

async def check_and_update_superlinks(session):
    superlinks = await db.load_superlinks()
    valid_superlinks = {}

    for link, value in superlinks.items():
        while BOT_STOPPED:
            print(f"[LOG] Bot stopped. Sleeping for 10 minutes...")
            await asyncio.sleep(600)
            await check_duolingo_access()
            continue
            
        is_valid = await check_link_validity(link, session)
        await asyncio.sleep(2)
        if is_valid:
            valid_superlinks[link] = value

    await db.save_superlinks(valid_superlinks)
    
async def shorten_url_yeumoney(url: str, session: aiohttp.ClientSession):
    try:
        encoded_url = quote(url)
        api_url = f"https://yeumoney.com/QL_api.php?token={YEUMONEY_TOKEN}&url={encoded_url}&format=json"
        
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(api_url, headers=headers) as response:
            if response.status == 200:
                text = await response.text()
                try:
                    data = json.loads(text)
                    if data["status"] == "success":
                        return data["shortenedUrl"]
                    else:
                        print(f"[LOG] Yeumoney error status: {data['status']}")
                        return None
                except json.JSONDecodeError as e:
                    print(f"[LOG] Yeumoney JSON decode error: {e}")
                    return None
    except Exception as e:
        print(f"[LOG] Yeumoney error: {e}")
        return None

async def shorten_url_link4m(url: str, session: aiohttp.ClientSession):
    try:
        encoded_url = quote(url)
        api_url = f"https://link4m.co/api-shorten/v2?api={LINK4M_TOKEN}&url={encoded_url}"
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(api_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if data["status"] == "success":
                    return data["shortenedUrl"]
                else:
                    print(f"[LOG] Link4m error message: {data.get('message')}")
                    return None
    except Exception as e:
        print(f"[LOG] Link4m error: {e}")
    return None

async def shorten_url_link2m(url: str, session: aiohttp.ClientSession):
    try:
        encoded_url = quote(url)
        api_url = f"https://link2m.net/api-shorten/v2?api={LINK2M_TOKEN}&url={encoded_url}"
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(api_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if data["status"] == "success":
                    return data["shortenedUrl"]
                else:
                    print(f"[LOG] Link2m error message: {data.get('message')}")
                    return None
    except Exception as e:
        print(f"[LOG] Link2m error: {e}")
    return None

async def shorten_url_clksh(url: str, session: aiohttp.ClientSession):
    try:
        encoded_url = quote(url)
        api_url = f"https://clk.sh/api?api={CLKSH_TOKEN}&url={encoded_url}"
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(api_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if data["status"] != "error":
                    return data["shortenedUrl"]
                else:
                    print(f"[LOG] Clk.sh error message: {data.get('message')}")
                    return None
    except Exception as e:
        print(f"[LOG] Clk.sh error: {e}")
    return None

async def shorten_url_shrinkearn(url: str, session: aiohttp.ClientSession):
    try:
        encoded_url = quote(url)
        api_url = f"https://shrinkearn.com/api?api={SHRINKEARN_TOKEN}&url={encoded_url}"
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(api_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if data["status"] != "error":
                    return data["shortenedUrl"]
                else:
                    print(f"[LOG] Shrinkearn error message: {data.get('message')}")
                    return None
    except Exception as e:
        print(f"[LOG] Shrinkearn error: {e}")
    return None

async def shorten_url_seotrieuview(url: str, session: aiohttp.ClientSession):
    try:
        encoded_url = quote(url)
        api_url = f"https://www.seotrieuview.com/api/api-develop?token={SEOTRIEUVIEW_TOKEN}&url={encoded_url}&format=json&alisa="
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        async with session.get(api_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("success"):
                    return data["shortenedUrl"]
                else:
                    print(f"[LOG] Seotrieuview error message: {data.get('message', '')}")
                    return None
    except Exception as e:
        print(f"[LOG] Seotrieuview error: {e}")
    return None

@bot.tree.command(name="free", description="Get DuoXPy Free Premium access for 1 day")
@is_in_guild()
@custom_cooldown(4, CMD_COOLDOWN)
async def free_v3(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    is_premium = await is_premium_user(interaction.user)
    if is_premium:
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Already Premium",
            description=f"{FAIL_EMOJI} You already have premium access.",
            color=discord.Color.red()
        )
        embed.set_author(
            name=interaction.user.display_name, 
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.followup.send(embed=embed)
        return
    try:
        selected_profile = await db.get_selected_profile(interaction.user.id)
        if not selected_profile:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} No Duolingo account selected. Please use link first.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.followup.send(embed=embed)
            return

        timezone = selected_profile.get('timezone', 'Asia/Saigon')
        vietnam_timezones = ['Asia/Ho_Chi_Minh', 'Asia/Saigon', 'Asia/Bangkok']
        is_vietnam = timezone in vietnam_timezones

        key = generate_key("free")
        expires_at = (datetime.now(pytz.UTC) + timedelta(days=1)).timestamp()
        await db.keys.insert_one({
            "_id": key,
            "type": "free",
            "duration": 1
        })
        await db.keys.create_index(
            "created_at", 
            expireAfterSeconds=86400,
            partialFilterExpression={"used_by": None}
        )

        base_url = f"https://www.duoxpy.site/get-key?key={key}"
        
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Free Key",
            description=f"{CHECK_EMOJI} Generating URLs...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.followup.send(embed=wait_embed)
        async with await get_session(direct=True) as session:
            path = []
            if is_vietnam:
                first_short = await shorten_url_yeumoney(base_url, session)
                path.append("Yeumoney")
                if not first_short:
                    raise Exception("Failed at first Yeumoney shortening")

                second_short = await shorten_url_yeumoney(first_short, session)
                path.append("Yeumoney")
                if not second_short:
                    raise Exception("Failed at second Yeumoney shortening")

                final_short = await shorten_url_yeumoney(second_short, session)
                path.append("Yeumoney")
                if not final_short:
                    raise Exception("Failed at third Yeumoney shortening")
            else:
                first_short = await shorten_url_clksh(base_url, session)
                path.append("Clk.sh")
                if not first_short:
                    raise Exception("Failed at first Clk.sh shortening")

                second_short = await shorten_url_clksh(first_short, session)
                path.append("Clk.sh")
                if not second_short:
                    raise Exception("Failed at second Clk.sh shortening")

                third_short = await shorten_url_shrinkearn(second_short, session)
                path.append("Shrinkearn")
                if not third_short:
                    raise Exception("Failed at first Shrinkearn shortening")

                final_short = await shorten_url_shrinkearn(third_short, session)
                path.append("Shrinkearn")
                if not final_short:
                    raise Exception("Failed at second Shrinkearn shortening")

            path_description = " -> ".join(reversed(path))
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Free Key", 
                description=(
                    f"{EYES_EMOJI} Link: {final_short}\n"
                    f"{CHECK_EMOJI} Path: {path_description}"
                ),
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)

    except Exception as e:
        error_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Error", 
            description=f"{FAIL_EMOJI} An error occurred",
            color=discord.Color.red()
        )
        error_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=error_embed)

@tasks.loop(minutes=6)
async def check_expired_keys():
    current_time = datetime.now(timezone.utc)
    guild = bot.get_guild(SERVER)
    if not guild:
        print("[LOG] Guild not found")
        return

    premium_role = guild.get_role(PRE_ROLE)
    free_role = guild.get_role(FREE_ROLE)
    ultra_role = guild.get_role(ULTRA_ROLE)
    if not premium_role or not free_role or not ultra_role:
        print("[LOG] Could not find premium, free or ultra role")
        return

    async for user_data in db.discord.find({"premium.active": True}):
        user_id = user_data["_id"]
        premium_data = user_data.get("premium", {})
        premium_type = premium_data.get("type")
        end_time = premium_data.get("end")

        member = guild.get_member(user_id)
        if not member:
            continue

        if premium_type != "lifetime" and end_time:
            end_time = datetime.fromtimestamp(end_time, tz=timezone.utc)
            if end_time < current_time:
                await db.discord.update_one(
                    {"_id": user_id},
                    {"$set": {
                        "premium.active": False,
                        "premium.type": None,
                        "premium.duration": None,
                        "premium.start": None,
                        "premium.end": None
                    }}
                )
                
                if premium_role in member.roles:
                    try:
                        await member.remove_roles(premium_role)
                        print(f"[LOG] Removed premium role from user {user_id} due to expiration")
                    except discord.HTTPException as e:
                        print(f"[LOG] Failed to remove premium role from {member.name}#{member.discriminator} (ID: {member.id}): {e}")
                
                if free_role in member.roles:
                    try:
                        await member.remove_roles(free_role)
                        print(f"[LOG] Removed free role from user {user_id} due to expiration")
                    except discord.HTTPException as e:
                        print(f"[LOG] Failed to remove free role from {member.name}#{member.discriminator} (ID: {member.id}): {e}")

                if ultra_role in member.roles:
                    try:
                        await member.remove_roles(ultra_role)
                        print(f"[LOG] Removed ultra role from user {user_id} due to expiration")
                    except discord.HTTPException as e:
                        print(f"[LOG] Failed to remove ultra role from {member.name}#{member.discriminator} (ID: {member.id}): {e}")
                continue

        has_premium = premium_type in ["monthly", "lifetime"]
        has_free = premium_type == "free"
        has_ultra = premium_type == "ultra"

        if has_premium and premium_role not in member.roles:
            try:
                await member.add_roles(premium_role)
                print(f"[LOG] Added premium role to user {user_id}")
            except discord.HTTPException as e:
                print(f"[LOG] Failed to add premium role to {member.name}#{member.discriminator} (ID: {member.id}): {e}")
        elif not has_premium and premium_role in member.roles:
            try:
                await member.remove_roles(premium_role)
                print(f"[LOG] Removed premium role from user {user_id}")
            except discord.HTTPException as e:
                print(f"[LOG] Failed to remove premium role from {member.name}#{member.discriminator} (ID: {member.id}): {e}")

        if has_free and free_role not in member.roles:
            try:
                await member.add_roles(free_role)
                print(f"[LOG] Added free role to user {user_id}")
            except discord.HTTPException as e:
                print(f"[LOG] Failed to add free role to {member.name}#{member.discriminator} (ID: {member.id}): {e}")
        elif not has_free and free_role in member.roles:
            try:
                await member.remove_roles(free_role)
                print(f"[LOG] Removed free role from user {user_id}")
            except discord.HTTPException as e:
                print(f"[LOG] Failed to remove free role from {member.name}#{member.discriminator} (ID: {member.id}): {e}")

        if has_ultra and ultra_role not in member.roles:
            try:
                await member.add_roles(ultra_role)
                print(f"[LOG] Added ultra role to user {user_id}")
            except discord.HTTPException as e:
                print(f"[LOG] Failed to add ultra role to {member.name}#{member.discriminator} (ID: {member.id}): {e}")
        elif not has_ultra and ultra_role in member.roles:
            try:
                await member.remove_roles(ultra_role)
                print(f"[LOG] Removed ultra role from user {user_id}")
            except discord.HTTPException as e:
                print(f"[LOG] Failed to remove ultra role from {member.name}#{member.discriminator} (ID: {member.id}): {e}")

def generate_key(key_type, duration=None):
    key_prefix = f"DUOXPY-{'ULTRA' if key_type == 'ultra' else 'LIFETIME' if key_type == 'lifetime' else 'FREE' if key_type == 'free' else f'{duration}MONTH'}"
    random_part = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(15))
    return f"{key_prefix}-{random_part}"

class KeyView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False
        self.key_type = None
        self.amount = None
        self.generated_keys = []
        self.key_chunks = []
        self.embeds = []

    async def setup(self):
        key_types = [
            discord.SelectOption(label="1 Month", value="1month", emoji=GEM_EMOJI),
            discord.SelectOption(label="3 Months", value="3month", emoji=GEM_EMOJI),
            discord.SelectOption(label="6 Months", value="6month", emoji=GEM_EMOJI),
            discord.SelectOption(label="12 Months", value="12month", emoji=GEM_EMOJI),
            discord.SelectOption(label="Lifetime", value="lifetime", emoji=DIAMOND_TROPHY_EMOJI),
            discord.SelectOption(label="Ultra", value="ultra", emoji=DIAMOND_TROPHY_EMOJI)
        ]

        type_select = discord.ui.Select(
            placeholder="Select key type",
            options=key_types,
            custom_id="key_type"
        )
        type_select.callback = self.type_callback
        self.add_item(type_select)

    async def type_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.key_type = interaction.data['values'][0]

        amounts = [
            discord.SelectOption(label=str(i), value=str(i), emoji=TRASH_EMOJI)
            for i in [1, 5, 10, 20, 30, 40, 50]
        ]

        amount_select = discord.ui.Select(
            placeholder="Select amount",
            options=amounts,
            custom_id="amount"
        )
        amount_select.callback = self.amount_callback

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Key Generator",
            description="",
            color=discord.Color.green()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )

        view = discord.ui.View(timeout=60)
        view.add_item(amount_select)
        await interaction.edit_original_response(embed=embed, view=view)

    async def amount_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.amount = int(interaction.data['values'][0])

        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Generating keys...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)

        for _ in range(self.amount):
            if self.key_type in ["lifetime", "ultra"]:
                duration = None
                new_key = generate_key(self.key_type)
                await db.keys.insert_one({
                    "_id": new_key,
                    "type": self.key_type,
                    "duration": duration
                })
            else:
                duration = int(self.key_type.split("month")[0])
                new_key = generate_key("monthly", duration)
                await db.keys.insert_one({
                    "_id": new_key,
                    "type": "monthly", 
                    "duration": duration
                })
            self.generated_keys.append(new_key)

        self.key_chunks = [self.generated_keys[i:i + 10] for i in range(0, len(self.generated_keys), 10)]
        
        for page in range(len(self.key_chunks)):
            all_keys_text = "\n".join(self.key_chunks[page])
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Generated Keys (**{self.key_type}**)",
                description=f"{CHECK_EMOJI} Generated **{self.amount}** key(s) of type: **{self.key_type}**\nPage {page + 1}/{len(self.key_chunks)}\n```fix\n{all_keys_text}\n```",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            self.embeds.append(embed)

        nav_view = NavigationView(self.embeds)
        await interaction.edit_original_response(embed=self.embeds[0], view=nav_view)
        nav_view.message = await interaction.original_response()
        self.command_completed = True
        self.stop()

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)

@bot.tree.command(name="redeem", description="Redeem or gift a Premium key")
@app_commands.describe(
    key="The premium key to redeem",
    gift="The user to gift this key to"
)
@is_in_guild()
@custom_cooldown(4, CMD_COOLDOWN)
async def redeem_v3(interaction: discord.Interaction, key: str, gift: Optional[discord.User] = None):
    target_user = gift if gift else interaction.user
    
    try:
        await interaction.response.defer(ephemeral=True)
        
        user_id = str(target_user.id)
        is_premium = await is_premium_user(target_user)
        is_ultra = await is_ultra_user(target_user)
        
        key_data = await db.keys.find_one({"_id": key})
        if not key_data:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Invalid Key",
                description=f"{FAIL_EMOJI} Invalid key.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=target_user.display_name,
                icon_url=target_user.avatar.url if target_user.avatar else target_user.display_avatar.url
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        if is_premium and key_data['type'] != 'ultra':
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Already Premium",
                description=f"{FAIL_EMOJI} {'The user' if gift else 'You'} already {'has' if gift else 'have'} premium access.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=target_user.display_name,
                icon_url=target_user.avatar.url if target_user.avatar else target_user.display_avatar.url
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        if is_ultra and key_data['type'] == 'ultra':
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Already Ultra",
                description=f"{FAIL_EMOJI} {'The user' if gift else 'You'} already {'has' if gift else 'have'} ultra access.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=target_user.display_name,
                icon_url=target_user.avatar.url if target_user.avatar else target_user.display_avatar.url
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        start_time = datetime.now(timezone.utc).timestamp()
        end_time = None
        if key_data['type'] == 'monthly':
            end_time = start_time + (30 * key_data['duration'] * 24 * 60 * 60)
        elif key_data['type'] == 'free':
            end_time = start_time + (24 * 60 * 60)

        await db.discord.update_one(
            {"_id": int(user_id)},
            {"$set": {
                "premium": {
                    "active": True,
                    "type": key_data['type'],
                    "duration": key_data['duration'],
                    "start": start_time,
                    "end": end_time
                }
            }}
        )

        await db.keys.delete_one({"_id": key})

        if key_data['type'] == 'free':
            current_time = time.time()
            await db.loop_usage.update_one(
                {"_id": user_id},
                {"$set": {
                    "loops_left": LOOPS,
                    "last_used": current_time
                }},
                upsert=True
            )
        
        guild = bot.get_guild(SERVER)
        target_user_in_server = guild.get_member(target_user.id)
        
        if target_user_in_server:
            if key_data['type'] == 'ultra':
                ultra_role = guild.get_role(ULTRA_ROLE)
                if ultra_role:
                    await target_user_in_server.add_roles(ultra_role)
            elif key_data['type'] == 'free':
                free_role = guild.get_role(FREE_ROLE)
                if free_role:
                    await target_user_in_server.add_roles(free_role)
            else:
                premium_role = guild.get_role(PRE_ROLE)
                if premium_role:
                    await target_user_in_server.add_roles(premium_role)

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Key {'Gifted' if gift else 'Redeemed'}",
            description=f"{CHECK_EMOJI} {'You have successfully gifted' if gift else 'You have successfully redeemed'} a **{key_data['type']}** key{f' to {gift.mention}' if gift else ''}!",
            color=discord.Color.green()
        )
        embed.set_author(
            name=target_user.display_name,
            icon_url=target_user.avatar.url if target_user.avatar else target_user.display_avatar.url
        )
        
        if end_time:
            expiry_date = datetime.fromtimestamp(end_time, tz=timezone.utc)
            embed.add_field(name="Expires On", value=expiry_date.strftime("%Y-%m-%d %H:%M:%S"), inline=False)
        else:
            embed.add_field(name="Expires On", value="Never (Lifetime)", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    except Exception as e:
        error_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Error",
            description=f"{FAIL_EMOJI} An error occurred",
            color=discord.Color.red()
        )
        error_embed.set_author(
            name=target_user.display_name,
            icon_url=target_user.avatar.url if target_user.avatar else target_user.display_avatar.url
        )
        await interaction.followup.send(embed=error_embed, ephemeral=True)
          
async def getheaders(token: str, userid: str):
    misc = Miscellaneous()
    user_agent = misc.randomize_mobile_user_agent()
    
    headers = {
        "accept": "application/json", 
        "authorization": f"Bearer {token}",
        "connection": "Keep-Alive",
        "content-type": "application/json",
        "cookie": f"jwt_token={token}",
        "origin": "https://www.duolingo.com",
        "user-agent": user_agent,
        "x-amzn-trace-id": f"User={userid}",
    }
    return headers

async def decode_jwt(jwt):
    try:
        parts = jwt.split('.')
        if len(parts) != 3:
            raise ValueError("Invalid JWT format")
        
        _, payload, _ = parts
        decoded = base64.urlsafe_b64decode(payload + "==")
        return json.loads(decoded)
    
    except Exception as e:
        print(f"[LOG] Error decoding JWT: {e}")
        return None

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    print(f"- Version: {VERSION}")
    print(f"- Codename: {CODENAME}")
    try:
        synced = await bot.tree.sync()
        print(f'[LOG] Synced {len(synced)} command(s)')
    except Exception as e:
        print(f'[LOG] Error syncing commands: {e}')
        
    check_expired_keys.start()
    user_count_channel.start()
    xp_farmers_channel.start()
    streak_farmers_channel.start()
    gem_farmers_channel.start()
    streak_savers_channel.start()
    league_savers_channel.start()
    league_farmers_channel.start()
    duo_accounts_channel.start()
    follow_accounts_channel.start()
    quest_savers_channel.start()
    bot_status_channel.start()
    cleanup_old_tasks.start()
    bot.loop.create_task(smart_check())

@tasks.loop(minutes=60)
async def cleanup_old_tasks():
    now = datetime.now(pytz.UTC)
    to_delete = []
    for task_id, task in list(TASKS.items()):
        if task.end_time and (now - task.end_time) > timedelta(hours=24):
            to_delete.append(task_id)
    for task_id in to_delete:
        del TASKS[task_id]

@bot.tree.command(name="about", description="About DuoXPy Max")
@is_in_guild()
@custom_cooldown(4, CMD_COOLDOWN)
async def about_v3(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    embed = discord.Embed(
        title=f"{MAX_EMOJI} About DuoXPy Max",
        description=(
            f"**Version Info**\n"
            f"- Version: **`{VERSION}`**\n"
            f"- Codename: **`{CODENAME}`**\n\n"
            "**Special Thanks** \n"
            "- **[GorouFlex](https://github.com/gorouflex/)** - Developer\n"
            "- **[smhaa](https://github.com/Chromeyc/)** - Developer, Supporter\n"
            "- **xiaojia6** - Developer, Advisor\n"
            "- **`sen_can`** - SuperLink Provider, Donator, Supporter\n\n"
            f"[Install DuoXPy Max]({AUTH_URL})"
        ),
        color=discord.Color.brand_green()
    )
    
    embed.set_thumbnail(url=bot.user.avatar.url)
    embed.set_footer(text="Made with 💚 by the DuoXPy Team", icon_url=bot.user.avatar.url)
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
    )
    
    await interaction.followup.send(embed=embed, ephemeral=True)
      
class LogoutView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.message = None
        self.command_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = [discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI) for profile in self.profiles]
            
            if options:
                select = discord.ui.Select(
                    placeholder="Select accounts to unlink",
                    options=options,
                    max_values=len(options),
                    min_values=1,
                    custom_id="select_accounts_logout"
                )
                select.callback = self.logout_callback
                self.add_item(select)

    async def logout_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)
        
        self.command_completed = True
        self.stop()
        selected_ids = interaction.data['values']
        selected_profiles = [p for p in self.profiles if str(p["_id"]) in selected_ids]
        
        if selected_profiles:
            currently_selected = await db.get_selected_profile(self.user_id)
            extra_msg = ""
            
            for profile in selected_profiles:
                await db.duolingo.delete_one({"_id": profile["_id"]})
            
            remaining_profiles = await db.list_profiles(self.user_id)
            
            if not remaining_profiles:
                await db.select_profile(self.user_id, None)
            elif currently_selected and str(currently_selected["_id"]) in selected_ids and remaining_profiles:
                new_profile = remaining_profiles[0]
                await db.select_profile(self.user_id, new_profile["_id"])
                extra_msg = f"\nAutomatically selected **{new_profile.get('username', 'None')}** as your selected profile."

            usernames = []
            for profile in selected_profiles:
                username = profile.get("username")
                if username:
                    usernames.append(username)
                else:
                    usernames.append("None")

            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Unlinked",
                description=f"{CHECK_EMOJI} Successfully unlinked {len(usernames)} account(s):\n**{', '.join(usernames)}**{extra_msg}",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )

            await interaction.edit_original_response(embed=embed, view=None)
            self.stop()
        else:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Unlink",
                description=f"{FAIL_EMOJI} No accounts were selected.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            self.stop()

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()

class StreakSaverView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        if discord_profile:
            current_status = discord_profile.get("streaksaver", False)
            
            options = [
                discord.SelectOption(
                    label="Enable Streak Saver" if not current_status else "Disable Streak Saver",
                    description="Status: " + ("Enabled" if current_status else "Disabled"),
                    value="toggle", 
                    emoji=CHECK_EMOJI if not current_status else FAIL_EMOJI
                )
            ]

            select = discord.ui.Select(
                placeholder="Configure Streak Saver",
                options=options,
                custom_id="select_streaksaver"
            )
            select.callback = self.streaksaver_callback
            self.add_item(select)
    
    async def streaksaver_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.command_completed = True

        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)
        
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        current_status = discord_profile.get("streaksaver", False) if discord_profile else False
        new_status = not current_status
        
        await db.discord.update_one(
            {"_id": self.user_id},
            {"$set": {"streaksaver": new_status}},
            upsert=True
        )
        
        status_text = "enabled" if new_status else "disabled"
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Streak Saver {STREAK_EMOJI}",
            description=f"{CHECK_EMOJI} Streak saver has been **{status_text}**.",
            color=discord.Color.green() if new_status else discord.Color.red()
        )
        
        embed.set_author(
            name=interaction.user.display_name, 
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)
        self.stop()

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()
        
async def getduolingoinfo(user_id, session):
    url = f"https://www.duolingo.com/2017-06-30/users/{user_id}"
    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()
        else:
            return None

async def getduoinfo(user_id, jwt_token, session):
    url = f"https://www.duolingo.com/2017-06-30/users/{user_id}"
    headers = await getheaders(jwt_token, user_id)
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        else:
            return None

class InfoView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False
        self.current_page = 0
        self.courses = []
        self.course_chunks = []
        self.total_pages = 0
        self.embeds = []
        self.interaction = None

    async def setup(self, interaction):
        self.interaction = interaction
        selected_profile = await db.get_selected_profile(self.user_id)
        if not selected_profile:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} No selected account. Please select an account.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.interaction.user.display_name,
                icon_url=self.interaction.user.avatar.url if self.interaction.user.avatar else self.interaction.user.display_avatar.url
            )
            await self.interaction.edit_original_response(embed=embed, view=None)
            return False
        if selected_profile['paused'] is True:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Account Paused",
                description=f"{FAIL_EMOJI} Your account is currently paused. Please update your account credentials using `/account`",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.interaction.user.display_name,
                icon_url=self.interaction.user.avatar.url if self.interaction.user.avatar else self.interaction.user.display_avatar.url
            )
            await self.interaction.edit_original_response(embed=embed, view=None)
            return False
            
        self.duolingo_username = selected_profile['username']
        self.jwt_token = selected_profile['jwt_token'] 
        self.duo_id = selected_profile['_id']
        self.hide = selected_profile.get('hide', False)
        self.timezone = selected_profile.get('timezone', 'Asia/Saigon')

        discord_profile = await db.discord.find_one({"_id": self.user_id})
        if discord_profile:
            self.streaksaver = discord_profile.get('streaksaver', False)
            self.questsaver = discord_profile.get('questsaver', False)
            self.autoleague = discord_profile.get('autoleague', {'active': False, 'target': None, 'autoblock': False})
            self.autoblock = self.autoleague.get('autoblock', False)
        else:
            self.streaksaver = False
            self.questsaver = False
            self.autoleague = {'active': False, 'target': None, 'autoblock': False}
            self.autoblock = False

        self.is_premium = await is_premium_user(await bot.fetch_user(self.user_id))
        self.is_free = await is_premium_free_user(await bot.fetch_user(self.user_id))
        async with await get_session(direct=True) as session:
            self.duo_info = await getduoinfo(self.duo_id, self.jwt_token, session)
            if not self.duo_info:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Could not fetch account info.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=self.interaction.user.display_name,
                    icon_url=self.interaction.user.avatar.url if self.interaction.user.avatar else self.interaction.user.display_avatar.url
                )
                await self.interaction.edit_original_response(embed=embed, view=None)
                return False
            self.profilepic = await getimageurl(self.duo_info)
            self.avatar_file = None
            if (self.is_free or not self.is_premium) and not await check_name_match(self.duo_info):
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Account Info", 
                    description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=self.duo_info.get('name', 'Unknown'),
                    icon_url=self.profilepic
                )
                await self.interaction.edit_original_response(embed=embed, view=None)
                return False
            if self.profilepic:
                try:
                    async with session.get(self.profilepic) as response:
                        if response.status == 200:
                            image_data = await response.read()
                            self.avatar_file = discord.File(io.BytesIO(image_data), filename='avatar.png')
                except Exception as e:
                    print(f"Error downloading avatar: {e}")

            from_date = datetime.now(pytz.timezone(self.timezone)).strftime('%Y-%m-%d')
            fixed_number = "1722561352306"
            api_url = f"https://duolingo.com/2017-06-30/users/{self.duo_id}/xp_summaries?startDate={from_date}&_={fixed_number}"
            try:
                async with session.get(api_url, headers=await getheaders(self.jwt_token, self.duo_id)) as response:
                    self.xp_data = await response.json() if response.status == 200 else None
            except Exception as e:
                print(f"Error fetching XP history: {e}")
                self.xp_data = None

        self.courses = self.duo_info.get("courses", [])
        self.course_chunks = [self.courses[i:i + 5] for i in range(0, len(self.courses), 5)]
        self.total_pages = max(2, len(self.course_chunks) + 1)
        main_embed = await self.create_main_embed()
        self.embeds.append(main_embed)
        profile_embed = await self.create_profile_embed()
        self.embeds.append(profile_embed)
        for i, chunk in enumerate(self.course_chunks):
            course_embed = await self.create_course_embed(chunk, i)
            self.embeds.append(course_embed)
            
        nav_view = NavigationView(self.embeds)
        nav_view.message = await self.interaction.edit_original_response(embed=self.embeds[0], attachments=[self.avatar_file], view=nav_view)
        return True

    async def create_main_embed(self):
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max Account Info",
            description=f"[{self.duolingo_username}](https://www.duolingo.com/profile/{self.duolingo_username})",
            color=discord.Color.green()
        )
        if self.profilepic:
            embed.set_author(
                name=self.duo_info.get('name', 'Unknown'),
                icon_url=self.profilepic
            )
            if self.avatar_file:
                embed.set_thumbnail(url="attachment://avatar.png")
                
        embed.add_field(name=f"{CHECK_EMOJI} JWT Token", value=f"||{self.jwt_token}||", inline=False)
        
        if self.is_premium:
            discord_profile = await db.discord.find_one({"_id": self.user_id})
            if discord_profile and "premium" in discord_profile:
                premium_data = discord_profile["premium"]
                if premium_data["active"]:
                    start_time = datetime.fromtimestamp(premium_data["start"], tz=timezone.utc) if premium_data.get("start") else None
                    end_time = datetime.fromtimestamp(premium_data["end"], tz=timezone.utc) if premium_data.get("end") else None
                    
                    if premium_data["type"] == "monthly" and end_time:
                        embed.add_field(name=f"{MAX_EMOJI} DuoXPy Max Premium",
                                      value=f"```fix\nMonthly - {end_time.strftime('%Y-%m-%d %H:%M:%S')}```",
                                      inline=False)
                    elif premium_data["type"] == "lifetime":
                        embed.add_field(name=f"{MAX_EMOJI} DuoXPy Max Premium",
                                      value="```fix\nLifetime - Never Expires```",
                                      inline=False)
                    elif premium_data["type"] == "free":
                        if end_time:
                            embed.add_field(name=f"{MAX_EMOJI} DuoXPy Max Premium",
                                          value=f"```fix\nFree - {end_time.strftime('%Y-%m-%d %H:%M:%S')}```",
                                          inline=False)
                        else:
                            embed.add_field(name=f"{MAX_EMOJI} DuoXPy Max Premium",
                                          value="```fix\nFree - No expiry date```",
                                          inline=False)
                    else:
                        embed.add_field(name=f"{MAX_EMOJI} DuoXPy Max Premium",
                                      value="```fix\nActive```",
                                      inline=False)
            elif self.interaction.guild and self.interaction.user in self.interaction.guild.premium_subscribers:
                embed.add_field(name=f"{MAX_EMOJI} DuoXPy Max Premium",
                              value="```fix\nBooster```",
                              inline=False)
                              
        embed.add_field(name=f"{HIDE_EMOJI} Hide",
                      value=f"```fix\n{'True' if self.hide else 'False'}```",
                      inline=True)
        embed.add_field(name=f"{STREAK_EMOJI} StreakSaver",
                      value=f"```fix\n{'True' if self.streaksaver else 'False'}```",
                      inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{QUEST_EMOJI} QuestSaver",
                      value=f"```fix\n{'True' if self.questsaver else 'False'}```",
                      inline=True)
        embed.add_field(name=f"{DIAMOND_TROPHY_EMOJI} LeagueSaver",
                      value=f"```fix\n{'True' if self.autoleague['active'] else 'False'}```",
                      inline=True)
        embed.add_field(name="", value="", inline=False)
        
        if self.autoleague['active'] and self.autoleague['target']:
            embed.add_field(name=f"{DIAMOND_TROPHY_EMOJI} League Target",
                          value=f"```fix\n{self.autoleague['target']}```",
                          inline=True)
            embed.add_field(name=f"{DUO_MAD_EMOJI} Auto Block",
                          value=f"```fix\n{'True' if self.autoblock else 'False'}```",
                          inline=True)
                          
        embed.add_field(name=f"{CALENDAR_EMOJI} Timezone", value=f"```fix\n{self.timezone}```", inline=False)
        return embed

    async def create_profile_embed(self):
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max Profile Info",
            description=f"[{self.duolingo_username}](https://www.duolingo.com/profile/{self.duolingo_username})",
            color=discord.Color.green()
        )
        if self.profilepic:
            embed.set_author(
                name=self.duo_info.get('name', 'Unknown'),
                icon_url=self.profilepic
            )
            if self.avatar_file:
                embed.set_thumbnail(url="attachment://avatar.png")
                
        embed.add_field(name=f"{EYES_EMOJI} User ID", value=f"```fix\n{self.duo_id}```", inline=True)
        embed.add_field(name=f"{NERD_EMOJI} Username", value=f"```fix\n{self.duolingo_username}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{HOME_EMOJI} Nickname", value=f"```fix\n{self.duo_info.get('name', 'Unknown')}```", inline=True)
        embed.add_field(name=f"{DUO_MAD_EMOJI} Moderator", value=f"```fix\n{self.duo_info.get('canUseModerationTools', 'Unknown')}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{SUPER_EMOJI} Super Duolingo", value=f"```fix\n{self.duo_info.get('hasPlus', 'Unknown')}```", inline=True)
        embed.add_field(name=f"{CHECK_EMOJI} Email", value=f"```fix\n{self.duo_info.get('email', 'Unknown')}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{CHECK_EMOJI} Email Verified", value=f"```fix\n{self.duo_info.get('emailVerified', 'Unknown')}```", inline=True)
        
        timestamp = self.duo_info.get("creationDate")
        if timestamp:
            date_time = datetime.fromtimestamp(timestamp, pytz.timezone(self.timezone))
            readable_date = date_time.strftime('%d %B %Y')
        else:
            readable_date = 'Unknown'
            
        embed.add_field(name=f"{CALENDAR_EMOJI} Creation Day", value=f"```fix\n{readable_date}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{STREAK_EMOJI} Streak", value=f"```fix\n{self.duo_info.get('streak', 'Unknown')}```", inline=True)
        embed.add_field(name=f"{XP_EMOJI} Total XP", value=f"```fix\n{self.duo_info.get('totalXp', 'Unknown')}```", inline=True)
        
        if self.xp_data:
            summaries = self.xp_data.get('summaries', [])
            if summaries:
                embed.add_field(name=f"{DIAMOND_TROPHY_EMOJI} Recent XP History", value="", inline=False)
                for entry in summaries[:1]:
                    gained_xp = entry.get('gainedXp', 0)
                    date_str = datetime.fromtimestamp(entry.get('date', 0), tz=pytz.timezone(self.timezone)).strftime('%Y-%m-%d %H:%M:%S')
                    num_sessions = entry.get('numSessions', 0)
                    total_session_time = entry.get('totalSessionTime', 0)
                    hours = total_session_time // 3600
                    minutes = (total_session_time % 3600) // 60
                    seconds = total_session_time % 60

                    time_str = ""
                    if hours > 0:
                        time_str += f"{hours}h "
                    if minutes > 0:
                        time_str += f"{minutes}m "
                    if seconds > 0 or not time_str:
                        time_str += f"{seconds}s"

                    embed.add_field(name=f"{CALENDAR_EMOJI} Date", value=f"```fix\n{date_str}```", inline=True)
                    embed.add_field(name=f"{XP_EMOJI} XP Gained", value=f"```fix\n{gained_xp}```", inline=True)
                    embed.add_field(name="", value="", inline=False)
                    embed.add_field(name=f"{DUOLINGO_TRAINING_EMOJI} Sessions", value=f"```fix\n{num_sessions}```", inline=True)
                    embed.add_field(name=f"{EYES_EMOJI} Total Time", value=f"```fix\n{time_str}```", inline=True)
                    
        return embed
           
    async def create_course_embed(self, courses, page_num):
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max Languages",
            description=f"[{self.duolingo_username}](https://www.duolingo.com/profile/{self.duolingo_username})",
            color=discord.Color.green()
        )
        if self.profilepic:
            embed.set_author(
                name=self.duo_info.get('name', 'Unknown'),
                icon_url=self.profilepic
            )
            if self.avatar_file:
                embed.set_thumbnail(url="attachment://avatar.png")
                
        embed.add_field(name=f"{GLOBE_DUOLINGO_EMOJI} Languages", value="", inline=False)
        
        for course in courses:
            language_code = course.get('learningLanguage', '').upper()
            from_code = course.get('fromLanguage', '').upper()
            embed.add_field(
                name=f"{DUOLINGO_TRAINING_EMOJI} {course.get('title', 'Unknown')} [{from_code}] ({DIAMOND_TROPHY_EMOJI}: {course.get('crowns', 'Unknown')}) ({XP_EMOJI} XP: {course.get('xp', 'Unknown')})",
                value="",
                inline=False
            )
            
        return embed

class SelectAccount(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.message = None
        self.has_selected = False

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        selected_profile = await db.get_selected_profile(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                username = profile.get("username", "None") or "None"
                is_selected = selected_profile and profile['_id'] == selected_profile['_id']
                
                label = f"{username}"
                description = "Selected" if is_selected else None
                
                options.append(
                    discord.SelectOption(
                        label=label,
                        value=str(profile['_id']),
                        emoji=EYES_EMOJI,
                        description=description
                    )
                )
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account"
            )
            select.callback = self.select_account_callback
            self.add_item(select)

    async def select_account_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Switching accounts...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)
        
        self.has_selected = True
        selected_id = int(interaction.data['values'][0])
        await db.select_profile(self.user_id, selected_id)
        
        selected_profile = next((profile for profile in self.profiles if profile['_id'] == selected_id), None)
        if selected_profile:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Selected Account",
                description=f"{CHECK_EMOJI} Selected account: **{selected_profile['username']}**",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

    async def on_timeout(self):
        if not self.has_selected:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None
                           
class StartView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if str(interaction.user.id) != str(self.user_id):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Access Denied", 
                description=f"{FAIL_EMOJI} This menu is only for <@{self.user_id}>",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return False
        return True

    async def setup(self):
        options = [
            discord.SelectOption(
                label="XP",
                description="Farm XP (499 XP per loop)",
                value="xp",
                emoji=XP_EMOJI
            ),
            discord.SelectOption(
                label="Gem",
                description="Farm Gems (120 Gem per loop)", 
                value="gem",
                emoji=GEM_EMOJI
            ),
            discord.SelectOption(
                label="Streak",
                description="Farm Streak (1 Streak per loop)",
                value="streak", 
                emoji=STREAK_EMOJI
            ),
            discord.SelectOption(
                label="League",
                description="Farm League position",
                value="league",
                emoji=DIAMOND_TROPHY_EMOJI
            ),
            discord.SelectOption(
                label="Follow",
                description="Make all bot accounts in our Database follow you",
                value="follow",
                emoji=EYES_EMOJI
            ),
            discord.SelectOption(
                label="Unfollow", 
                description="Unfollow users you've followed",
                value="unfollow",
                emoji=TRASH_EMOJI
            ),
            discord.SelectOption(
                label="Block",
                description="Block users in current Leaderboard",
                value="block",
                emoji=DUO_MAD_EMOJI
            ),
            discord.SelectOption(
                label="Unblock",
                description="Unblock users you've blocked",
                value="unblock",
                emoji=NERD_EMOJI
            )
        ]
        
        select = discord.ui.Select(
            placeholder="Select task type",
            options=options,
            custom_id="select_task"
        )
        select.callback = self.task_callback
        self.add_item(select)

    async def task_callback(self, interaction: discord.Interaction):
        try:
            task_type = interaction.data['values'][0]
            
            if task_type in ["league"]:
                if await is_premium_free_user(interaction.user):
                    self.command_completed = True
                    self.stop()
                    await interaction.response.defer()
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Access Denied",
                        description=f"{FAIL_EMOJI} This command is only available to DuoXPy Paid Premium users.\n{GEM_EMOJI} You can purchase our premium plan at {SHOP}.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

            if task_type in ["xp", "gem", "streak"]:
                modal = AmountModal(task_type, self.message.id, self)
                await interaction.response.send_modal(modal)
            elif task_type == "league":
                modal = PositionModal(self.message.id, self)
                await interaction.response.send_modal(modal)
            else:
                self.command_completed = True
                self.stop()
                await interaction.response.defer()
                await start_task(interaction, task_type, message_id=self.message.id)
                
        except Exception as e:
            print(f"[LOG] Error in task_callback: {e}")
            self.command_completed = True
            self.stop()
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            try:
                await interaction.edit_original_response(embed=embed, view=None)
            except discord.NotFound:
                pass
    async def on_timeout(self):
        if not self.command_completed:
            try:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                    description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                    color=discord.Color.red()
                )
                if self.message:
                    embed.set_author(
                        name=self.message.interaction_metadata.user.display_name,
                        icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                    )
                    await self.message.edit(embed=embed, view=None)
            except discord.NotFound:
                pass
        self.stop()

class AmountModal(discord.ui.Modal):
    def __init__(self, task_type, message_id, view):
        super().__init__(title=f"Enter {task_type.capitalize()} Amount")
        self.task_type = task_type
        self.message_id = message_id
        self.view = view
        
        placeholder = f"Enter a number between 0-10000"
        max_length = 5
            
        self.amount = discord.ui.TextInput(
            label="Amount",
            placeholder=placeholder,
            min_length=1,
            max_length=max_length,
            required=True
        )
        self.add_item(self.amount)

    async def on_submit(self, interaction: discord.Interaction):
        self.view.command_completed = True
        self.view.stop()
        try:
            amount = int(self.amount.value)
            is_ultra = await is_ultra_user(interaction.user)
            
            if amount == 0 and not is_ultra:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Ultra Only Feature",
                    description=f"{FAIL_EMOJI} Setting amount to 0 is only available for DuoXPy Ultra users.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.response.edit_message(embed=embed, view=None)
                return
                
            if (is_ultra and amount >= 0 and amount <= 10000) or (not is_ultra and amount >= 1 and amount <= 10000):
                await interaction.response.defer(ephemeral=True)
                try:
                    await start_task(interaction, self.task_type, amount=amount, message_id=self.message_id)
                except Exception as e:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} An error occurred while processing your request",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
            else:
                raise ValueError
        except ValueError:
            is_ultra = await is_ultra_user(interaction.user)
            min_value = "0" if is_ultra else "1"
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Invalid Amount",
                description=f"{FAIL_EMOJI} Please enter a valid number between {min_value}-10000.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.edit_message(embed=embed, view=None)

class PositionModal(discord.ui.Modal):
    def __init__(self, message_id, view):
        super().__init__(title="Enter League Position")
        self.message_id = message_id
        self.view = view
        self.position = discord.ui.TextInput(
            label="Position",
            placeholder="Enter a number between 1-15",
            min_length=1,
            max_length=2,
            required=True
        )
        self.add_item(self.position)

    async def on_submit(self, interaction: discord.Interaction):
        self.view.command_completed = True
        self.view.stop()
        try:
            position = int(self.position.value)
            if 1 <= position <= 15:
                await interaction.response.defer(ephemeral=True)
                try:
                    await start_task(interaction, "league", position=position, message_id=self.message_id)
                except Exception as e:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
            else:
                raise ValueError
        except ValueError:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Invalid Position", 
                description=f"{FAIL_EMOJI} Please enter a valid number between 1-15.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.edit_message(embed=embed, view=None)

async def start_task(interaction: discord.Interaction, type, amount=None, position=None, message_id=None):
    if not await check_and_set_farm_status(interaction, type, message_id):
        return

    user_id = str(interaction.user.id)
    current_task = asyncio.current_task()
    process_data = {"message_id": message_id}

    try:
        profile = await db.get_selected_profile(interaction.user.id)
        if not profile:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: No Selected Account", 
                description=f"{FAIL_EMOJI} You need to log in first using link in `/account`.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return     
        if profile.get("paused", False):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Account Paused",
                description=f"{FAIL_EMOJI} Your account is currently paused. Please update your account credentials using `/account`",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return
    
        if type in ["xp", "gem", "streak"] and await is_premium_free_user(interaction.user):
            usage_data = await db.loop_usage.find_one({"_id": user_id})
            current_time = time.time()
            update = False
            if not usage_data:
                usage_data = {
                    "_id": user_id,
                    "loops_left": LOOPS,
                    "last_used": current_time,
                    "cooldown": LOOP_COOLDOWN
                }
                await db.loop_usage.insert_one(usage_data)
            else:
                time_since_last_used = current_time - usage_data.get("last_used", 0)
                if time_since_last_used >= LOOP_COOLDOWN:
                    usage_data["loops_left"] = LOOPS
                    usage_data["last_used"] = current_time
                    await db.loop_usage.update_one(
                        {"_id": user_id},
                        {"$set": {
                            "loops_left": LOOPS,
                            "last_used": current_time
                        }}
                    )
                    update = True
            if update:
                usage_data = await db.loop_usage.find_one({"_id": user_id})
            if usage_data["loops_left"] <= 0 or amount > usage_data["loops_left"]:
                time_since_last_used = current_time - usage_data["last_used"]
                time_until_reset = max(0, LOOP_COOLDOWN - time_since_last_used)

                next_hours = int(time_until_reset // 3600)
                next_minutes = int((time_until_reset % 3600) // 60)
                next_seconds = int(time_until_reset % 60)

                next_time_str = ""
                if next_hours > 0:
                    next_time_str += f"{next_hours}h "
                if next_minutes > 0:
                    next_time_str += f"{next_minutes}m "
                if next_seconds > 0 or not next_time_str:
                    next_time_str += f"{next_seconds}s"
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Insufficient Value",
                    description=(
                        f"{FAIL_EMOJI} You only have **{usage_data['loops_left']}** loops available.\n"
                        f"{EYES_EMOJI} Loops will reset to **{LOOPS}** in **{next_time_str}**."
                    ),
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return

            await db.loop_usage.update_one(
                {"_id": user_id},
                {"$set": {
                    "loops_left": usage_data["loops_left"] - amount
                }}
            )

        type_emoji = {
            "xp": XP_EMOJI,
            "gem": GEM_EMOJI,
            "streak": STREAK_EMOJI,
            "follow": EYES_EMOJI,
            "unfollow": TRASH_EMOJI,
            "block": DUO_MAD_EMOJI,
            "unblock": NERD_EMOJI,
            "league": DIAMOND_TROPHY_EMOJI
        }[type]

        type_title = type.capitalize()

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: {type_title} {type_emoji}",
            description=f"{CHECK_EMOJI} Starting {type} process...",
            color=discord.Color.teal()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)

        task = Task(interaction.user.id, profile["_id"], type, amount, message_id, position)
        TASKS[task.task_id] = task

        try:
            if type == "xp":
                await process_xp_farm(interaction, amount, process_data, profile)
            elif type == "gem":
                await process_gem_farm(interaction, amount, process_data, profile)
            elif type == "streak":
                await process_streak_farm(interaction, amount, process_data, profile)
            elif type == "follow":
                await process_follow_farm(interaction, process_data, profile)
            elif type == "unfollow":
                await process_massunfollow(interaction, process_data, profile)
            elif type == "block":
                await process_block_farm(interaction, process_data, profile)
            elif type == "unblock":
                await process_massunblock(interaction, process_data, profile)
            elif type == "league":
                await process_league_farm(interaction, process_data, position, profile)

        except asyncio.CancelledError:
            for t in TASKS.values():
                if str(t.discord_id) == str(interaction.user.id) and t.status == "running":
                    t.status = "cancelled"
                    t.end_time = datetime.now(pytz.UTC)
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: {type_title} Cancelled {type_emoji}",
                description=f"{FAIL_EMOJI} {type_title} task has been cancelled by user.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            try:
                await interaction.edit_original_response(embed=embed, view=None)
            except:
                pass
            return

    finally:
        for t in TASKS.values():
            if str(t.discord_id) == str(interaction.user.id) and t.status == "cancelled":
                t.end_time = datetime.now(pytz.UTC)
                type_emoji = {
                    "xp": XP_EMOJI,
                    "gem": GEM_EMOJI,
                    "streak": STREAK_EMOJI,
                    "follow": EYES_EMOJI,
                    "unfollow": TRASH_EMOJI,
                    "block": DUO_MAD_EMOJI,
                    "unblock": NERD_EMOJI,
                    "league": DIAMOND_TROPHY_EMOJI
                }[type]
                type_title = type.capitalize()
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: {type_title} Cancelled {type_emoji}",
                    description=f"{FAIL_EMOJI} {type_title} task has been cancelled by user.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                try:
                    await interaction.edit_original_response(embed=embed, view=None)
                except:
                    return
                break
        return

async def process_gem_farm(interaction: discord.Interaction, loop_count: int, process_data: dict, profile: dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id")
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    timezone = profile.get("timezone", "Asia/Saigon")
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Gem {GEM_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Gem {GEM_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "gem" and t.status == "running":
                task_obj = t
                break
        if task_obj:
            task_obj.progress = 0
            task_obj.total = "inf" if loop_count == 0 else loop_count
            task_obj.estimated_time_left = None

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Gem {GEM_EMOJI}",
            description=f"{CHECK_EMOJI} Task started! Use `/task` to check progress.",
            color=discord.Color.teal()
        )
        if hide:
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
        else:
            embed.set_author(name=duo_username, icon_url=duo_avatar)
        await interaction.edit_original_response(embed=embed)

        while retry_attempts < 10:
            try:
                gems = 0
                start_time = time.time()
                fromLanguage = duo_info.get('fromLanguage', 'Unknown')
                learningLanguage = duo_info.get('learningLanguage', 'Unknown')
                reward_types = [
                    "SKILL_COMPLETION_BALANCED-3cc66443_c14d_3965_a68b_e4eb1cfae15e-2-GEMS",
                    "SKILL_COMPLETION_BALANCED-110f61a1_f8bc_350f_ac25_1ded90c1d2ed-2-GEMS"
                ]

                i = 0
                while True:
                    if task_obj and task_obj.status != "running":
                        break
                    if loop_count > 0 and i >= loop_count:
                        break

                    gems += 120
                    random.shuffle(reward_types)
                    for _ in range(2):
                        for reward_type in reward_types:
                            url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/rewards/{reward_type}"
                            payload = {"consumed": True, "fromLanguage": fromLanguage, "learningLanguage": learningLanguage}
                            retry_count = 0
                            while retry_count < 10:
                                async with session.patch(url, headers=headers, json=payload) as response:
                                    if response.status == 200:
                                        retry_count = 0
                                        break
                                    else:
                                        retry_count += 1
                                        if retry_count < 10:
                                            await asyncio.sleep(60)
                                        else:
                                            raise Exception(f"Failed to farm gems after 10 attempts. Status: {response.status}")
                    i += 1
                    if task_obj:
                        elapsed_time = time.time() - start_time
                        task_obj.progress = i
                        task_obj.estimated_time_left = None
                        if i > 0 and loop_count > 0:
                            estimated_total_time = (elapsed_time / i) * loop_count
                            estimated_remaining_time = estimated_total_time - elapsed_time
                            task_obj.estimated_time_left = estimated_remaining_time
                        task_obj.gained = gems

                total_time = time.time() - start_time
                hours, remainder = divmod(int(total_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h " if hours > 0 else ""
                time_str += f"{minutes}m " if minutes > 0 else ""
                time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                if task_obj:
                    task_obj.progress = i
                    task_obj.estimated_time_left = 0
                    task_obj.gained = gems
                    task_obj.end_time = datetime.now(pytz.UTC)
                    if not task_obj.status == "cancelled":
                        task_obj.status = "completed"

                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Gem Success {GEM_EMOJI}",
                    description=f"",
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                final_embed.add_field(name="Total Gained", value=gems, inline=True)
                final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                if hide:
                    final_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                if success_channel:
                    await success_channel.send(embed=final_embed)
                break

            except Exception as error:
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Gem Error {GEM_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred during gem farm after 10 retry attempts",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'gems' in locals() and gems > 0:
                        error_embed.add_field(name="Total Gained", value=gems, inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    print(error)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                else:
                    await asyncio.sleep(60)

async def save_streak(session, profile, headers, duo_info=None):
    retry_attempts = 0
    discord_id = profile.get("discord_id")
    duo_id = profile["_id"]
    try:
        while retry_attempts < 10:
            try:
                timezone_str = profile.get("timezone", "Asia/Saigon")
                discord_profile = await db.discord.find_one({"_id": discord_id})
                hide = discord_profile.get("hide", False)
                duo_username = duo_info.get('name', 'Unknown') if duo_info else 'Unknown'
                duo_avatar = await getimageurl(duo_info) if duo_info else None
                try:
                    user_tz = pytz.timezone(timezone_str)
                except pytz.exceptions.UnknownTimeZoneError:
                    user_tz = pytz.timezone("Asia/Saigon")
                now = datetime.now(user_tz)
                streak_data = duo_info.get('streakData', {})
                current_streak = streak_data.get('currentStreak', {})
                should_do_lesson = True
                if current_streak:
                    last_extended = current_streak.get('lastExtendedDate')
                    if last_extended:
                        last_extended = datetime.strptime(last_extended, "%Y-%m-%d")
                        last_extended = user_tz.localize(last_extended)
                        should_do_lesson = last_extended.date() < now.date()
                if not should_do_lesson:
                    break
                task = Task(discord_id, duo_id, "streaksaver", None, None)
                TASKS[task.task_id] = task
                
                fromLanguage = duo_info.get('fromLanguage', 'Unknown')
                learningLanguage = duo_info.get('learningLanguage', 'Unknown')
                session_payload = {
                    "challengeTypes": [
                        "assist", "characterIntro", "characterMatch", "characterPuzzle",
                        "characterSelect", "characterTrace", "characterWrite",
                        "completeReverseTranslation", "definition", "dialogue",
                        "extendedMatch", "extendedListenMatch", "form", "freeResponse",
                        "gapFill", "judge", "listen", "listenComplete", "listenMatch",
                        "match", "name", "listenComprehension", "listenIsolation",
                        "listenSpeak", "listenTap", "orderTapComplete", "partialListen",
                        "partialReverseTranslate", "patternTapComplete", "radioBinary",
                        "radioImageSelect", "radioListenMatch", "radioListenRecognize",
                        "radioSelect", "readComprehension", "reverseAssist",
                        "sameDifferent", "select", "selectPronunciation",
                        "selectTranscription", "svgPuzzle", "syllableTap",
                        "syllableListenTap", "speak", "tapCloze", "tapClozeTable",
                        "tapComplete", "tapCompleteTable", "tapDescribe", "translate",
                        "transliterate", "transliterationAssist", "typeCloze",
                        "typeClozeTable", "typeComplete", "typeCompleteTable",
                        "writeComprehension"
                    ],
                    "fromLanguage": fromLanguage,
                    "isFinalLevel": False,
                    "isV2": True,
                    "juicy": True,
                    "learningLanguage": learningLanguage,
                    "smartTipsVersion": 2,
                    "type": "GLOBAL_PRACTICE"
                }
                session_url = "https://www.duolingo.com/2017-06-30/sessions"
                async with session.post(session_url, headers=headers, json=session_payload) as response:
                    if response.status in [400, 404, 401]:
                        break
                    elif response.status != 200:
                        error_channel = bot.get_channel(ERROR_ID)
                        if error_channel:
                            error_embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: StreakSaver Error {STREAK_EMOJI}",
                                description=f"{FAIL_EMOJI} Failed to create session for <@{discord_id}>: Status {response.status}",
                                color=discord.Color.red(),
                                timestamp=datetime.now()
                            )
                            user = await bot.fetch_user(discord_id)
                            if user:
                                if hide:
                                    error_embed.set_author(
                                        name=user.display_name,
                                        icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                                    )
                                else:
                                    error_embed.set_author(
                                        name=duo_username,
                                        icon_url=duo_avatar
                                    )
                            await error_channel.send(embed=error_embed)
                        retry_attempts += 1
                        if retry_attempts < 10:
                            await asyncio.sleep(60)
                            continue
                        break
                    session_data = await response.json()
                if 'id' not in session_data:
                    retry_attempts += 1
                    if retry_attempts < 10:
                        await asyncio.sleep(60)
                        continue
                    break
                start_time = now.timestamp()
                end_time = datetime.now(user_tz).timestamp()
                update_payload = {
                    **session_data,
                    "heartsLeft": 5,
                    "startTime": start_time,
                    "endTime": end_time,
                    "enableBonusPoints": False,
                    "failed": False,
                    "maxInLessonStreak": 9,
                    "shouldLearnThings": True
                }
                update_url = f"https://www.duolingo.com/2017-06-30/sessions/{session_data['id']}"
                async with session.put(update_url, headers=headers, json=update_payload) as response:
                    if response.status in [400, 404, 401]:
                        retry_attempts += 1
                        if retry_attempts < 10:
                            await asyncio.sleep(60)
                            continue
                        break
                    elif response.status != 200:
                        error_channel = bot.get_channel(ERROR_ID)
                        if error_channel:
                            error_embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: StreakSaver Error {STREAK_EMOJI}",
                                description=f"{FAIL_EMOJI} Failed to update session for <@{discord_id}>: Status {response.status}",
                                color=discord.Color.red(),
                                timestamp=datetime.now()
                            )
                            user = await bot.fetch_user(discord_id)
                            if user:
                                if hide:
                                    error_embed.set_author(
                                        name=user.display_name,
                                        icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                                    )
                                else:
                                    error_embed.set_author(
                                        name=duo_username,
                                        icon_url=duo_avatar
                                    )
                            await error_channel.send(embed=error_embed)
                        retry_attempts += 1
                        if retry_attempts < 10:
                            await asyncio.sleep(60)
                            continue
                        break
                    update_response = await response.json()
                if update_response.get('xpGain') is not None:
                    success_channel = bot.get_channel(SUCCESS_ID)
                    if success_channel:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: StreakSaver {STREAK_EMOJI}",
                            description=f"{CHECK_EMOJI} Successfully saved streak for <@{discord_id}>",
                            color=discord.Color.orange(),
                            timestamp=datetime.now()
                        )
                        user = await bot.fetch_user(discord_id)
                        if user:
                            if hide:
                                embed.set_author(
                                    name=user.display_name,
                                    icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                                )
                            else:
                                embed.set_author(
                                    name=duo_username,
                                    icon_url=duo_avatar
                                )
                        await success_channel.send(embed=embed)
                    if not task.status == "cancelled":
                        task.status = "completed"
                    task.end_time = datetime.now(pytz.UTC)
                    return True
                retry_attempts += 1
                if retry_attempts < 10:
                    await asyncio.sleep(60)
                    continue
                break
            except Exception as error:
                error_channel = bot.get_channel(ERROR_ID)
                if error_channel:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: StreakSaver Error {STREAK_EMOJI}",
                        description=f"{FAIL_EMOJI} Error saving streak for <@{discord_id}>",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    user = await bot.fetch_user(discord_id)
                    if user:
                        if hide:
                            error_embed.set_author(
                                name=user.display_name,
                                icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                            )
                        else:
                            error_embed.set_author(
                                name=duo_username,
                                icon_url=duo_avatar
                            )
                    await error_channel.send(embed=error_embed)
                print(f"[LOG] Error in save_streak for user {discord_id}: {error}")
                retry_attempts += 1
                if retry_attempts < 10:
                    await asyncio.sleep(60)
                    continue
                break
    finally:
        if 'task' in locals() and task.status == "running":
            if not task.status == "cancelled":
                task.status = "completed"
            task.end_time = datetime.now(pytz.UTC)

async def save_quests(session, profile, headers, current_info=None):
    retry_attempts = 0
    discord_id = profile.get("discord_id")
    duo_id = profile["_id"]
    try:
        while retry_attempts < 10:
            try:
                timezone_str = profile.get("timezone", "Asia/Saigon")
                discord_profile = await db.discord.find_one({"_id": discord_id})
                hide = discord_profile.get("hide", False)
                duo_username = current_info.get('name', 'Unknown') if current_info else 'Unknown'
                duo_avatar = await getimageurl(current_info) if current_info else None
                progress_url = f"https://goals-api.duolingo.com/users/{duo_id}/progress?timezone={timezone_str}"
                async with session.get(progress_url, headers=headers) as progress_response:
                    if progress_response.status != 200:
                        retry_attempts += 1
                        if retry_attempts < 10:
                            await asyncio.sleep(60)
                            continue
                        break
                    progress_data = await progress_response.json()
                    goals_progress = progress_data.get("goals", {}).get("progress", {})
                    try:
                        user_tz = pytz.timezone(timezone_str)
                    except pytz.exceptions.UnknownTimeZoneError:
                        user_tz = pytz.timezone("Asia/Saigon")
                    now = datetime.now(user_tz)
                    current_monthly_challenge = f"{now.year}_{now.month:02d}_monthly_challenge"
                    filtered_progress = {k: v for k, v in goals_progress.items() if k != current_monthly_challenge}
                    should_run = any(progress == 0 for progress in filtered_progress.values())
                    if not should_run:
                        break

                task = Task(discord_id, duo_id, "questsaver", None, None)
                TASKS[task.task_id] = task

                schema_url = "https://goals-api.duolingo.com/schema?ui_language=en"
                async with session.get(schema_url, headers=headers) as schema_response:
                    if schema_response.status != 200:
                        retry_attempts += 1
                        if retry_attempts < 10:
                            await asyncio.sleep(60)
                            continue
                        break
                    schema_data = await schema_response.json()
                    seen_metrics = {}
                    unique_metrics = []
                    for goal in schema_data.get("goals", []):
                        metric = goal.get("metric")
                        if metric and metric not in seen_metrics and metric != "QUESTS":
                            seen_metrics[metric] = True
                            unique_metrics.append(metric)
                try:
                    user_tz = pytz.timezone(timezone_str)
                except pytz.exceptions.UnknownTimeZoneError:
                    user_tz = pytz.timezone("Asia/Saigon")
                now = datetime.now(user_tz)
                formatted_time = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                metric_updates = [{"metric": metric, "quantity": 2000} for metric in unique_metrics]
                metric_updates.append({"metric": "QUESTS", "quantity": 1})
                json_data = {
                    "metric_updates": metric_updates,
                    "timestamp": formatted_time,
                    "timezone": timezone_str
                }
                url = f"https://goals-api.duolingo.com/users/{duo_id}/progress/batch"
                async with session.post(url, headers=headers, json=json_data) as response:
                    response_text = await response.text()
                    if response.status in [400, 404, 401]:
                        retry_attempts += 1
                        if retry_attempts < 10:
                            await asyncio.sleep(60)
                            continue
                        break
                    elif response.status != 200:
                        error_channel = bot.get_channel(ERROR_ID)
                        if error_channel:
                            error_embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: QuestSaver Error {QUEST_EMOJI}",
                                description=f"{FAIL_EMOJI} Failed to save quests for <@{discord_id}>: Status {response.status}",
                                color=discord.Color.red(),
                                timestamp=datetime.now()
                            )
                            user = await bot.fetch_user(discord_id)
                            if user:
                                if hide:
                                    error_embed.set_author(
                                        name=user.display_name,
                                        icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                                    )
                                else:
                                    error_embed.set_author(
                                        name=duo_username,
                                        icon_url=duo_avatar
                                    )
                            await error_channel.send(embed=error_embed)
                        retry_attempts += 1
                        if retry_attempts < 10:
                            await asyncio.sleep(60)
                            continue
                        break
                    if response_text == '{"message":"SUCCESS"}':
                        success_channel = bot.get_channel(SUCCESS_ID)
                        if success_channel:
                            embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: QuestSaver {QUEST_EMOJI}",
                                description=f"{CHECK_EMOJI} Successfully saved quests for <@{discord_id}>",
                                color=discord.Color.teal(),
                                timestamp=datetime.now()
                            )
                            user = await bot.fetch_user(discord_id)
                            if user:
                                if hide:
                                    embed.set_author(
                                        name=user.display_name,
                                        icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                                    )
                                else:
                                    embed.set_author(
                                        name=duo_username,
                                        icon_url=duo_avatar
                                    )
                            await success_channel.send(embed=embed)
                        if not task.status == "cancelled":
                            task.status = "completed"
                        task.end_time = datetime.now(pytz.UTC)
                        return True
                retry_attempts += 1
                if retry_attempts < 10:
                    await asyncio.sleep(60)
                    continue
                break
            except Exception as error:
                error_channel = bot.get_channel(ERROR_ID)
                if error_channel:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: QuestSaver Error {QUEST_EMOJI}",
                        description=f"{FAIL_EMOJI} Error saving quests for <@{discord_id}>",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    user = await bot.fetch_user(discord_id)
                    if user:
                        if hide:
                            error_embed.set_author(
                                name=user.display_name,
                                icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                            )
                        else:
                            error_embed.set_author(
                                name=duo_username,
                                icon_url=duo_avatar
                            )
                    await error_channel.send(embed=error_embed)
                print(f"[LOG] Error in save_quests for user {discord_id}: {error}")
                retry_attempts += 1
                if retry_attempts < 10:
                    await asyncio.sleep(60)
                    continue
                break
    finally:
        if 'task' in locals() and task.status == "running":
            if not task.status == "cancelled":
                task.status = "completed"
            task.end_time = datetime.now(pytz.UTC)
    
async def get_user_priority_chunks():
    users = await db.duolingo.find().to_list(None)
    user_groups = {}
    for user in users:
        discord_id = user.get('discord_id')
        if discord_id:
            if discord_id not in user_groups:
                user_groups[discord_id] = []
            user_groups[discord_id].append(user)
    
    high_priority = []
    mid_priority = []
    low_priority = []
    very_low_priority = []
    
    for discord_id, user_accounts in user_groups.items():
        account_count = len(user_accounts)
        if account_count <= 5:
            high_priority.extend(user_accounts)
        elif account_count <= 10:
            mid_priority.extend(user_accounts)
        elif account_count <= 15:
            low_priority.extend(user_accounts)
        else:
            very_low_priority.extend(user_accounts)
    
    high_priority.sort(key=lambda x: len(user_groups[x.get('discord_id', '')]))
    mid_priority.sort(key=lambda x: len(user_groups[x.get('discord_id', '')]))
    low_priority.sort(key=lambda x: len(user_groups[x.get('discord_id', '')]))
    very_low_priority.sort(key=lambda x: len(user_groups[x.get('discord_id', '')]))
    
    all_high_mid = high_priority + mid_priority
    all_low = low_priority + very_low_priority
    
    all_high_mid.sort(key=lambda x: len(user_groups[x.get('discord_id', '')]))
    all_low.sort(key=lambda x: len(user_groups[x.get('discord_id', '')]))
    
    min_users_per_chunk = max(1, len(all_high_mid) // 2)
    min_low_users_per_chunk = max(1, len(all_low) // 2)
    
    chunks = [[], [], [], []]
    chunks[0] = all_high_mid[:min_users_per_chunk]
    chunks[1] = all_high_mid[min_users_per_chunk:]
    chunks[2] = all_low[:min_low_users_per_chunk]
    chunks[3] = all_low[min_low_users_per_chunk:]    
    return chunks

async def process_chunk(chunk_index):
    while True:
        try:                
            chunks = await get_user_priority_chunks()
            chunk = chunks[chunk_index]
            
            discord_ids = list(set(user.get('discord_id') for user in chunk if user.get('discord_id')))
            discord_settings = {}
            async for doc in db.discord.find({"_id": {"$in": discord_ids}}):
                discord_settings[doc["_id"]] = doc

            guild = bot.get_guild(SERVER)
            
            async with await get_session(slot=chunk_index) as session:                
                for user in chunk:
                    try:
                        while BOT_STOPPED:
                            print(f"[LOG] Thread {chunk_index}: Bot stopped during user processing. Sleeping for 10 minutes...")
                            await asyncio.sleep(600)
                            await check_duolingo_access()
                            continue
                            
                        discord_id = user.get('discord_id')
                        if not discord_id:
                            continue
                        user_id = str(discord_id)
                        duo_id = user.get('_id')
                        if any(t.duolingo_id == duo_id and t.status == "running" for t in TASKS.values()):
                            print(f"[LOG] Thread {chunk_index}: User {duo_id} already has an active task. Skipping...")
                            continue
                        member = guild.get_member(discord_id)
                        if not member:
                            continue
                            
                        old_username = user.get('username')
                        old_timezone = user.get('timezone', 'Asia/Saigon')
                        jwt_token = user.get('jwt_token')
                        is_paused = user.get('paused', False)

                        if not jwt_token or not duo_id:
                            continue
                        headers = await getheaders(jwt_token, duo_id)
                        url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}"
                        
                        async with session.get(url, headers=headers) as response:
                            if response.status in [400, 404]:
                                if not is_paused:
                                    embed = discord.Embed(
                                        title=f"{MAX_EMOJI} DuoXPy Max: Account Issue Detected",
                                        description=(
                                            f"{FAIL_EMOJI} Your Duolingo account **{old_username}** has been removed from our database due to:\n"
                                            f"- Account banned/deactivated\n"
                                            f"- Password changed\n" 
                                            f"- Unable to verify account status\n\n"
                                            f"{QUEST_EMOJI} Please check your account and relink it using `/account`"
                                        ),
                                        timestamp=datetime.now(),
                                        color=discord.Color.red()
                                    )
                                    embed.set_author(
                                        name=member.display_name,
                                        icon_url=member.avatar.url if member.avatar else member.display_avatar.url
                                    )
                                    
                                    try:
                                        await member.send(embed=embed)
                                    except:
                                        print(f"[LOG] Thread {chunk_index}: Could not DM user {discord_id}")
                                    
                                    await db.duolingo.update_one({"_id": duo_id}, {"$set": {"paused": True}})
                                
                                await db.duolingo.delete_one({"_id": duo_id})
                                if discord_id:
                                    selected_profile = await db.get_selected_profile(discord_id)
                                    if selected_profile and selected_profile["_id"] == duo_id:
                                        remaining_profiles = await db.list_profiles(discord_id)
                                        if remaining_profiles:
                                            new_profile = remaining_profiles[0]
                                            await db.select_profile(discord_id, new_profile["_id"])
                                            print(f"[LOG] Thread {chunk_index}: Switched default profile for user {discord_id} to {new_profile['username']}")
                                        else:
                                            await db.select_profile(discord_id, None)
                                            print(f"[LOG] Thread {chunk_index}: Set selected profile to None for user {discord_id} as they have no remaining profiles")
                                continue
                            elif response.status == 401:
                                embed = discord.Embed(
                                    title=f"{MAX_EMOJI} DuoXPy Max: Account Issue Detected",
                                    description=(
                                        f"{FAIL_EMOJI} Your account **{old_username}** credentials have expired. Please update your credentials using `/account`"
                                    ),
                                    timestamp=datetime.now(),
                                    color=discord.Color.red()
                                )
                                embed.set_author(
                                    name=member.display_name,
                                    icon_url=member.avatar.url if member.avatar else member.display_avatar.url
                                )
                                
                                try:
                                    await member.send(embed=embed)
                                except:
                                    print(f"[LOG] Thread {chunk_index}: Could not DM user {discord_id}")
                                await asyncio.sleep(2)
                                continue
                            elif response.status != 200:
                                await asyncio.sleep(2)
                                continue
                                
                            current_info = await response.json()
                            new_username = current_info.get('username')
                            new_timezone = current_info.get('timezone', 'Asia/Saigon')
                            deactivated = current_info.get('deactivated')
                            display_name = current_info.get('name', '').lower()
                            normalized_name = unicodedata.normalize('NFKD', display_name)
                            normalized_name = ''.join(c for c in normalized_name if c.isalnum()).lower()
                            prohibited_patterns = [
                                r'.*a.*u.*t.*o.*d.*u.*o.*',
                                r'.*d.*u.*o.*s.*m.*',
                                r'.*c.*l.*i.*c.*k.*',
                                r'.*s.*u.*p.*e.*r.*d.*u.*o.*l.*i.*n.*g.*o.*',
                                r'.*f.*a.*m.*i.*l.*y.*',
                                r'.*s.*u.*p.*e.*r.*d.*u.*o.*',
                            ]
                            
                            is_prohibited = any(re.match(pattern, normalized_name) for pattern in prohibited_patterns)
                            
                            if is_prohibited:
                                print(f"[LOG] Thread {chunk_index}: Banned user {discord_id} for using prohibited display name: {display_name}")
                                try:
                                    await member.ban(reason=f"Using prohibited display name: {display_name}")
                                except Exception as e:
                                    print(f"[LOG] Thread {chunk_index}: Failed to ban user {discord_id}: {e}")
                                continue

                            if deactivated is None:
                                if not is_paused:
                                    embed = discord.Embed(
                                        title=f"{MAX_EMOJI} DuoXPy Max: Account Issue Detected",
                                        description=(
                                            f"{FAIL_EMOJI} Your account **{old_username}** credentials have expired. Please update your credentials using `/account`"
                                        ),
                                        timestamp=datetime.now(),
                                        color=discord.Color.red()
                                    )
                                    embed.set_author(
                                        name=member.display_name,
                                        icon_url=member.avatar.url if member.avatar else member.display_avatar.url
                                    )
                                    
                                    try:
                                        await member.send(embed=embed)
                                    except:
                                        print(f"[LOG] Thread {chunk_index}: Could not DM user {discord_id}")
                                    
                                    await db.duolingo.update_one({"_id": duo_id}, {"$set": {"paused": True}})
                                continue

                            if deactivated is True:
                                print(f"[LOG] Thread {chunk_index}: Removing deactivated account {old_username}")
                                embed = discord.Embed(
                                    title=f"{MAX_EMOJI} DuoXPy Max: Account Issue Detected",
                                    description=(
                                        f"{FAIL_EMOJI} Your Duolingo account **{old_username}** has been removed from our database due to:\n"
                                        f"- Account banned/deactivated\n\n"
                                        f"{QUEST_EMOJI} Please check your account and relink it using `/account`"
                                    ),
                                    timestamp=datetime.now(),
                                    color=discord.Color.red()
                                )
                                embed.set_author(
                                    name=member.display_name,
                                    icon_url=member.avatar.url if member.avatar else member.display_avatar.url
                                )
                                
                                try:
                                    await member.send(embed=embed)
                                except:
                                    print(f"[LOG] Thread {chunk_index}: Could not DM user {discord_id}")
                                
                                await db.duolingo.delete_one({"_id": duo_id})
                                if discord_id:
                                    selected_profile = await db.get_selected_profile(discord_id)
                                    if selected_profile and selected_profile["_id"] == duo_id:
                                        remaining_profiles = await db.list_profiles(discord_id)
                                        if remaining_profiles:
                                            new_profile = remaining_profiles[0]
                                            await db.select_profile(discord_id, new_profile["_id"])
                                            print(f"[LOG] Thread {chunk_index}: Switched default profile for user {discord_id} to {new_profile['username']}")
                                        else:
                                            await db.select_profile(discord_id, None)
                                            print(f"[LOG] Thread {chunk_index}: Set selected profile to None for user {discord_id} as they have no remaining profiles")
                                continue

                            if (new_username and new_username != old_username) or (new_timezone != old_timezone):
                                await db.duolingo.update_one(
                                    {"_id": duo_id},
                                    {"$set": {
                                        "username": new_username,
                                        "timezone": new_timezone,
                                        "paused": False
                                    }}
                                )

                            is_premium = await is_premium_user(member)
                            is_free = await is_premium_free_user(member)
                            await asyncio.sleep(2)
                            if not is_premium or is_free:
                                if current_info.get("name") != NAME:
                                    payload = {
                                        "name": NAME,
                                        "username": new_username or old_username,
                                        "email": user.get('email', ''),
                                        "signal": None
                                    }
                                    patch_url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}"
                                    async with session.patch(patch_url, headers=headers, json=payload) as patch_response:
                                        if patch_response.status != 200:
                                            await asyncio.sleep(2)
                                            continue
                            
                            discord_profile = discord_settings.get(discord_id, {})
                            if is_premium and not is_free:
                                if discord_profile.get("streaksaver", False):
                                    await save_streak(session, user, headers, current_info)
                                    await asyncio.sleep(2)
                                if discord_profile.get("questsaver", False):
                                    await save_quests(session, user, headers, current_info)
                                    await asyncio.sleep(2)
                                if discord_profile.get("autoleague", {}).get("active", False):
                                    await farm_league(session, user, headers, current_info)
                                    await asyncio.sleep(2)
                        await asyncio.sleep(2)

                    except asyncio.CancelledError:
                        continue
                    except Exception as e:
                        print(f"[LOG] Thread {chunk_index}: Error processing user {user.get('username')}: {e}")
                        continue

            await asyncio.sleep(600)
                    
        except asyncio.CancelledError:
            print(f"[LOG] Thread {chunk_index}: Thread cancelled, restarting...")
            continue
        except Exception as e:
            print(f"[LOG] Thread {chunk_index}: Error in process_chunk: {e}")
            await asyncio.sleep(2)
            continue

async def process_maintenance():
    while True:
        try:
            async with await get_session(direct=True) as session:
                await check_and_update_superlinks(session) 
                await create_follow_accounts()          
            await asyncio.sleep(600)
                    
        except asyncio.CancelledError:
            print("[LOG] Maintenance Thread: Thread cancelled, restarting...")
            continue
        except Exception as e:
            print(f"[LOG] Maintenance Thread: Error in process_maintenance: {e}")
            await asyncio.sleep(2)
            continue

async def smart_check():
    while True:
        tasks = []
        try:
            maintenance_task = asyncio.create_task(process_maintenance())
            tasks.append(maintenance_task)
            
            for i in range(4):
                task = asyncio.create_task(process_chunk(i))
                tasks.append(task)
            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"[LOG] Error in smart_check: {e}")
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                print(f"[LOG] Error cleaning up tasks: {e}")
        await asyncio.sleep(2)

async def extract_duolingo_user_to_id(username: str, session) -> int:
    url = f"https://www.duolingo.com/2017-06-30/users?username={username}"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            user_data = data.get("users", [])[0] if data.get("users") else None
            if user_data:
                return user_data.get("id")
    return None

async def create_follow_accounts():
    try:
        follow_creator = DuolingoAccountCreator()
        for _ in range(10):
            try:
                while BOT_STOPPED:
                    print(f"[LOG] Bot stopped. Sleeping for 10 minutes...")
                    await asyncio.sleep(600)
                    await check_duolingo_access()
                    continue
                password = "DuoXPy@2025"
                follow_account = await follow_creator.create_account(password)
                
                await db.follow.insert_one({
                    "_id": follow_account["_id"], 
                    "jwt_token": follow_account["jwt_token"]
                })
                
                await asyncio.sleep(2)
            except Exception as e:
                continue
    except Exception as e:
        print(f"[LOG] Error in create_follow_accounts: {e}")

@bot.tree.command(name="lookup", description="Lookup profile information")
@check_bot_running()
@is_in_guild()
@is_premium()
@is_paid_premium()
@custom_cooldown(4, CMD_COOLDOWN)
@app_commands.describe(
    duolingo="The Duolingo username to lookup",
    user="The Discord user to lookup"
)
async def lookup_v2(interaction: discord.Interaction, duolingo: str = None, user: discord.User = None):
    await interaction.response.defer(ephemeral=True)
    discord_user = user 
    username = duolingo 
    if username is None and discord_user is None:
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Lookup Error",
            description=f"{FAIL_EMOJI} Please provide either a Duolingo username or a Discord user.",
            color=discord.Color.red()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed)
        return
    if discord_user and not username:
        is_premium = await is_premium_user(discord_user)
        profiles = await db.list_profiles(discord_user.id)
        

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Account Info",
            color=discord.Color.green()
        )
        embed.set_author(
            name=discord_user.display_name,
            icon_url=discord_user.avatar.url if discord_user.avatar else discord_user.default_avatar.url
        )
        

        premium_status = "Yes" if is_premium else "No"
        embed.add_field(name=f"DuoXPy Premium", value=f"```fix\n{premium_status}```", inline=True)
        
        if not profiles:
            embed.add_field(name="", value=f"{FAIL_EMOJI} Account not found. They might not have logged in.", inline=False)
            embed.set_author(name=discord_user.display_name, icon_url=discord_user.avatar.url if discord_user.avatar else discord_user.default_avatar.url)
            await interaction.edit_original_response(embed=embed)
            return


        has_profiles = False
        for profile in profiles:
            username = profile["username"]
            discord_id = profile["discord_id"]
            discord_profile = await db.discord.find_one({"_id": discord_id})
            hide = discord_profile.get("hide", False)
            if hide and is_premium:
                continue

            profile_link = f"https://www.duolingo.com/profile/{username}" if not hide else "Hidden"
            embed.add_field(
                name="",
                value=f"- [{username}]({profile_link})",
                inline=False
            )
            has_profiles = True
            
        if not has_profiles:
            embed.description = f"{FAIL_EMOJI} No profiles found."

        await interaction.edit_original_response(embed=embed)
        return

    async with await get_session(direct=True) as session:
        duo_id = await extract_duolingo_user_to_id(username, session)
        if not duo_id:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: User not found",
                description=f"{FAIL_EMOJI} Could not find a user with the username **{username}**.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            return

        info = await getduolingoinfo(duo_id, session)
        if not info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Failed to Retrieve Info",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo user information. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            return

        profilepic = await getimageurl(info)
        if profilepic and not profilepic.startswith('https:'):
            profilepic = 'https:' + profilepic
        avatar_file = None
        if profilepic:
            try:
                async with session.get(profilepic) as response:
                    if response.status == 200:
                        image_data = await response.read()
                        avatar_file = discord.File(io.BytesIO(image_data), filename='avatar.png')
            except Exception as e:
                print(f"Error downloading avatar: {e}")
        duo_profile = await db.duolingo.find_one({"_id": duo_id})
        used_cheat = False
        hide = False
        account_owner_id = None
        if duo_profile:
            used_cheat = True
            discord_id = duo_profile.get("discord_id")
            discord_profile = await db.discord.find_one({"_id": discord_id})
            hide = discord_profile.get("hide", False)
            account_owner_id = discord_id

        from_date = datetime.now(pytz.UTC).strftime('%Y-%m-%d')
        fixed_number = "1722561352306"
        api_url = f"https://duolingo.com/2017-06-30/users/{duo_id}/xp_summaries?startDate={from_date}&_={fixed_number}"
        try:
            async with session.get(api_url) as response:
                xp_data = await response.json() if response.status == 200 else None
        except Exception as e:
            print(f"Error fetching XP history: {e}")
            xp_data = None
        embeds = []
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max Profile Info",
            description=f"User: [{username}](https://www.duolingo.com/profile/{username})",
            color=discord.Color.green()
        )
        if profilepic:
            embed.set_author(
                name=f"{info.get('name', 'Unknown')}",
                icon_url=profilepic,
            )
            embed.set_thumbnail(url=f"attachment://avatar.png")
        if used_cheat and not hide:
            owner_mention = f"<@{account_owner_id}>" if account_owner_id else "Unknown"
            embed.add_field(name=f"{TRASH_EMOJI} Account Owner", value=owner_mention, inline=True)

        embed.add_field(name=f"{EYES_EMOJI} User ID", value=f"```fix\n{duo_id}```", inline=True)
        embed.add_field(name=f"{NERD_EMOJI} Username", value=f"```fix\n{username}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{HOME_EMOJI} Nickname", value=f"```fix\n{info.get('name', 'Unknown')}```", inline=True)
        embed.add_field(name=f"{DUO_MAD_EMOJI} Moderator", value=f"```fix\n{info.get('canUseModerationTools', 'Unknown')}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{SUPER_EMOJI} Super Duolingo", value=f"```fix\n{info.get('hasPlus', 'Unknown')}```", inline=True)
        embed.add_field(name=f"{CHECK_EMOJI} Email Verified", value=f"```fix\n{info.get('emailVerified', 'Unknown')}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        timestamp = info.get("creationDate")
        readable_date = (
            datetime.fromtimestamp(timestamp, pytz.UTC).strftime('%d %B %Y')
            if timestamp else 'Unknown'
        )
        embed.add_field(name=f"{CALENDAR_EMOJI} Creation Day", value=f"```fix\n{readable_date}```", inline=True)
        embed.add_field(name="", value="", inline=False)
        embed.add_field(name=f"{STREAK_EMOJI} Streak", value=f"```fix\n{info.get('streak', 'Unknown')}```", inline=True)
        embed.add_field(name=f"{XP_EMOJI} Total XP", value=f"```fix\n{info.get('totalXp', 'Unknown')}```", inline=True)

        if xp_data:
            summaries = xp_data.get('summaries', [])
            if summaries:
                embed.add_field(name=f"{DIAMOND_TROPHY_EMOJI} Recent XP History", value="", inline=False)
                for entry in summaries[:1]:
                    gained_xp = entry.get('gainedXp', 0)
                    date_str = datetime.fromtimestamp(entry.get('date', 0), tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
                    num_sessions = entry.get('numSessions', 0)
                    total_session_time = entry.get('totalSessionTime', 0)
                    hours = total_session_time // 3600
                    minutes = (total_session_time % 3600) // 60
                    seconds = total_session_time % 60

                    time_str = ""
                    if hours > 0:
                        time_str += f"{hours}h "
                    if minutes > 0:
                        time_str += f"{minutes}m "
                    if seconds > 0 or not time_str:
                        time_str += f"{seconds}s"

                    embed.add_field(name=f"{CALENDAR_EMOJI} Date", value=f"```fix\n{date_str}```", inline=True)
                    embed.add_field(name=f"{DIAMOND_TROPHY_EMOJI} XP Gained", value=f"```fix\n{gained_xp}```", inline=True)
                    embed.add_field(name="", value="", inline=False)
                    embed.add_field(name=f"{DUOLINGO_TRAINING_EMOJI} Sessions", value=f"```fix\n{num_sessions}```", inline=True)
                    embed.add_field(name=f"{EYES_EMOJI} Total Time", value=f"```fix\n{time_str}```", inline=True)

        embeds.append(embed)
        courses = info.get("courses", [])
        course_chunks = [courses[i:i + 5] for i in range(0, len(courses), 5)]
        for i, chunk in enumerate(course_chunks):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max Profile Info",
                description=f"User: [{username}](https://www.duolingo.com/profile/{username})",
                color=discord.Color.green()
            )
            if profilepic:
                embed.set_author(
                    name=f"{info.get('name', 'Unknown')} ({username})",
                    icon_url=profilepic,
                )
                embed.set_thumbnail(url=f"attachment://avatar.png")
            
            embed.add_field(name=f"{GLOBE_DUOLINGO_EMOJI} Languages", value="", inline=False)
            for course in chunk:
                language_code = course.get('learningLanguage', '').upper()
                from_code = course.get('fromLanguage', '').upper()
                embed.add_field(
                    name=f"{DUOLINGO_TRAINING_EMOJI} {course.get('title', 'Unknown')} [{from_code}] ({DIAMOND_TROPHY_EMOJI}: {course.get('crowns', 'Unknown')}) ({XP_EMOJI} XP: {course.get('xp', 'Unknown')})",
                    value="",
                    inline=False
                )
            embeds.append(embed)
        view = NavigationView(embeds)
        view.message = await interaction.edit_original_response(attachments=[avatar_file], embed=embeds[0], view=view)
     
async def process_xp_farm(interaction: discord.Interaction, loops: int, process_data: dict, profile:dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id")
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    timezone = profile.get("timezone", "Asia/Saigon")
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: XP {XP_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: XP {XP_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "xp" and t.status == "running":
                task_obj = t
                break
        if task_obj:
            task_obj.progress = 0
            task_obj.total = "inf" if loops == 0 else loops
            task_obj.estimated_time_left = None

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: XP {XP_EMOJI}",
            description=f"{CHECK_EMOJI} Task started! Use `/task` to check progress.",
            color=discord.Color.teal()
        )
        if hide:
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
        else:
            embed.set_author(name=duo_username, icon_url=duo_avatar)
        await interaction.edit_original_response(embed=embed)

        while retry_attempts < 10:
            try:
                total_xp = 0
                start_time = time.time()
                i = 0

                while True:
                    if task_obj and task_obj.status != "running":
                        break
                    if loops > 0 and i >= loops:
                        break

                    current_time = datetime.now(pytz.timezone(timezone))
                    url = f'https://stories.duolingo.com/api2/stories/fr-en-le-passeport/complete'
                    dataget = {
                        "awardXp": True,
                        "completedBonusChallenge": True,
                        "fromLanguage": "en",
                        "hasXpBoost": False,
                        "illustrationFormat": "svg",
                        "isFeaturedStoryInPracticeHub": True,
                        "isLegendaryMode": True,
                        "isV2Redo": False,
                        "isV2Story": False,
                        "learningLanguage": "fr",
                        "masterVersion": True,
                        "maxScore": 0,
                        "score": 0,
                        "happyHourBonusXp": 469,
                        "startTime": current_time.timestamp(),
                        "endTime": datetime.now(pytz.timezone(timezone)).timestamp(),
                    }
                    retry_count = 0
                    while retry_count < 10:
                        async with session.post(url=url, headers=headers, json=dataget) as response:
                            if response.status == 200:
                                result = await response.json()
                                total_xp += result.get('awardedXp', 0)
                                retry_count = 0
                                break
                            else:
                                retry_count += 1
                                if retry_count < 10:
                                    await asyncio.sleep(60)
                                else:
                                    raise Exception(f"Failed to farm XP after 10 attempts. Status: {response.status}")

                    i += 1
                    if task_obj:
                        elapsed_time = time.time() - start_time
                        task_obj.progress = i
                        task_obj.estimated_time_left = None
                        if i > 0 and loops > 0:
                            estimated_total_time = (elapsed_time / i) * loops
                            estimated_remaining_time = estimated_total_time - elapsed_time
                            task_obj.estimated_time_left = estimated_remaining_time
                        task_obj.gained = total_xp

                total_time = time.time() - start_time
                hours, remainder = divmod(int(total_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h " if hours > 0 else ""
                time_str += f"{minutes}m " if minutes > 0 else ""
                time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                if task_obj:
                    task_obj.progress = i
                    task_obj.estimated_time_left = 0
                    task_obj.gained = total_xp
                    task_obj.end_time = datetime.now(pytz.UTC)
                    if not task_obj.status == "cancelled":
                        task_obj.status = "completed"

                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: XP Success {XP_EMOJI}",
                    description=f"",
                    color=discord.Color.gold(),
                    timestamp=datetime.now()
                )
                final_embed.add_field(name="Total Gained", value=total_xp, inline=True)
                final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                if hide:
                    final_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                if success_channel:
                    await success_channel.send(embed=final_embed)
                break

            except Exception as error:
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: XP Error {XP_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred during XP farm after 10 retry attempts",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'total_xp' in locals() and total_xp > 0:
                        error_embed.add_field(name="Total Gained", value=total_xp, inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    print(error)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                else:
                    await asyncio.sleep(60)

async def process_follow_farm(interaction: discord.Interaction, process_data: dict, profile: dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id") 
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    timezone = profile.get("timezone", "Asia/Saigon")
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Follow {EYES_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Follow {EYES_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "follow" and t.status == "running":
                task_obj = t
                break

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Follow {EYES_EMOJI}",
            description=f"{CHECK_EMOJI} Task started! Use `/task` to check progress.",
            color=discord.Color.teal()
        )
        if hide:
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
        else:
            embed.set_author(name=duo_username, icon_url=duo_avatar)
        await interaction.edit_original_response(embed=embed)

        while retry_attempts < 10:
            try:
                success_count = 0
                start_time = time.time()

                try:
                    target_duo_id = profile["_id"]
                    target_jwt_token = profile["jwt_token"]
                    target_headers = await getheaders(target_jwt_token, target_duo_id)
                    url = f"https://www.duolingo.com/2017-06-30/users/{target_duo_id}/privacy-settings"
                    async with session.get(url, headers=target_headers) as response:
                        if response.status != 200:
                            print(f"[LOG] Failed to get privacy status for user {target_duo_id}")
                            return
                        data = await response.json()
                        privacy_settings = data.get('privacySettings', [])
                        social_setting = next((setting for setting in privacy_settings if setting['id'] == 'disable_social'), None)
                        was_private = social_setting['enabled'] if social_setting else False
                    if was_private:
                        url = f"https://www.duolingo.com/2017-06-30/users/{target_duo_id}/privacy-settings?fields=privacySettings"
                        payload = {"DISABLE_SOCIAL": False}
                        async with session.patch(url, headers=target_headers, json=payload) as response:
                            if response.status != 200:
                                print(f"[LOG] Failed to set account to public for user {target_duo_id}")
                                return
                        await asyncio.sleep(2)

                    try:
                        url = f"https://www.duolingo.com/2017-06-30/friends/users/{target_duo_id}/followers?viewerId={target_duo_id}"
                        initial_follower_ids = set()
                        async with session.get(url, headers=target_headers) as response:
                            if response.status == 200:
                                data = await response.json()
                                followers = data.get('followers', {})
                                followers_list = followers.get('users', [])
                                initial_follower_ids = {user.get('userId') for user in followers_list}
                            else:
                                embed = discord.Embed(
                                    title=f"{MAX_EMOJI} DuoXPy Max: Follow Error {EYES_EMOJI}",
                                    description=f"{FAIL_EMOJI} Failed to get followers for your account.",
                                    color=discord.Color.red()
                                )
                                if hide:
                                    embed.set_author(
                                        name=interaction.user.display_name,
                                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                                    )
                                else:
                                    embed.set_author(
                                        name=duo_username,
                                        icon_url=duo_avatar
                                    )
                                await interaction.edit_original_response(embed=embed)
                                return

                        followbot_accounts = []
                        follow_accounts = await db.follow.find({}).to_list(None)
                        accounts_to_check = []
                        
                        for account in follow_accounts:
                            if account["_id"] not in initial_follower_ids and account["_id"] != target_duo_id:
                                followbot_accounts.append((account["jwt_token"], account["_id"]))
                                accounts_to_check.append(account["_id"])
                        total_accounts = len(followbot_accounts)

                        if total_accounts == 0:
                            embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: No Follow Accounts",
                                description=f"{FAIL_EMOJI} No accounts available to follow your account.",
                                color=discord.Color.red()
                            )
                            if hide:
                                embed.set_author(
                                    name=interaction.user.display_name,
                                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                                )
                            else:
                                embed.set_author(
                                    name=duo_username,
                                    icon_url=duo_avatar
                                )
                            await interaction.edit_original_response(embed=embed)
                            return

                        if task_obj:
                            task_obj.progress = 0
                            task_obj.total = total_accounts
                            task_obj.estimated_time_left = None

                        for i, (jwt_token, duo_id_use) in enumerate(followbot_accounts, start=1):
                            if task_obj and task_obj.status != "running":
                                break

                            try:
                                headers = await getheaders(jwt_token, duo_id_use)
                                current_time = datetime.now(pytz.timezone('Asia/Saigon'))
                                url = f'https://stories.duolingo.com/api2/stories/fr-en-le-passeport/complete'
                                dataget = {
                                    "awardXp": True,
                                    "completedBonusChallenge": True,
                                    "fromLanguage": "en",
                                    "hasXpBoost": False,
                                    "illustrationFormat": "svg",
                                    "isFeaturedStoryInPracticeHub": True,
                                    "isLegendaryMode": True,
                                    "isV2Redo": False,
                                    "isV2Story": False,
                                    "learningLanguage": "fr",
                                    "masterVersion": True,
                                    "maxScore": 0,
                                    "score": 0,
                                    "happyHourBonusXp": 469,
                                    "startTime": current_time.timestamp(),
                                    "endTime": datetime.now(pytz.timezone('Asia/Saigon')).timestamp(),
                                }
                                async with session.post(url=url, headers=headers, json=dataget) as response:
                                    if response.status != 200:
                                        await db.follow.delete_one({"_id": duo_id_use})
                                        continue  
                                await asyncio.sleep(1)
                                url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id_use}/follow/{target_duo_id}"
                                data = {"component": "profile_header_button"}
                                
                                async with session.post(url, headers=headers, json=data) as follow_response:
                                    if follow_response.status in [200, 201]:
                                        success_count += 1

                                if task_obj:
                                    elapsed_time = time.time() - start_time
                                    task_obj.progress = i
                                    task_obj.estimated_time_left = None
                                    if i > 0:
                                        estimated_total_time = (elapsed_time / i) * total_accounts
                                        estimated_remaining_time = estimated_total_time - elapsed_time
                                        task_obj.estimated_time_left = estimated_remaining_time
                                    task_obj.gained = success_count

                                await asyncio.sleep(1)
                            except Exception as e:
                                print(f"[LOG] Error processing account {duo_id_use}: {e}")
                                continue

                        total_time = time.time() - start_time
                        hours, remainder = divmod(int(total_time), 3600)
                        minutes, seconds = divmod(remainder, 60)
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                        if task_obj:
                            task_obj.progress = total_accounts
                            task_obj.estimated_time_left = 0
                            task_obj.gained = success_count
                            task_obj.end_time = datetime.now(pytz.UTC)
                            if not task_obj.status == "cancelled":
                                task_obj.status = "completed"

                        final_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Follow Success {EYES_EMOJI}",
                            description=f"{CHECK_EMOJI} Successfully followed account.",
                            color=discord.Color.green(),
                            timestamp=datetime.now()
                        )
                        final_embed.add_field(name="Accounts", value=f"{success_count}/{total_accounts}", inline=True)
                        final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                        if hide:
                            final_embed.set_author(
                                name=interaction.user.display_name,
                                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                            )
                        else:
                            final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                        if success_channel:
                            await success_channel.send(embed=final_embed)
                        break

                    finally:
                        if was_private:
                            try:
                                url = f"https://www.duolingo.com/2017-06-30/users/{target_duo_id}/privacy-settings?fields=privacySettings"
                                payload = {"DISABLE_SOCIAL": True}
                                async with session.patch(url, headers=target_headers, json=payload) as response:
                                    if response.status != 200:
                                        print(f"[LOG] Failed to restore private settings for user {target_duo_id}")
                            except Exception as e:
                                print(f"[LOG] Error restoring private settings for user {target_duo_id}: {e}")

                except Exception as error:
                    retry_attempts += 1
                    if retry_attempts >= 10:
                        error_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Follow Error {EYES_EMOJI}",
                            description=f"{FAIL_EMOJI} An error occurred during follow after 10 retry attempts",
                            color=discord.Color.red(),
                            timestamp=datetime.now()
                        )
                        if 'i' in locals() and i > 0:
                            error_embed.add_field(name="Accounts", value=f"{success_count}/{total_accounts}", inline=True)
                        if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                            time_str = f"{hours}h " if hours > 0 else ""
                            time_str += f"{minutes}m " if minutes > 0 else ""
                            time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                            error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                        if hide:
                            error_embed.set_author(
                                name=interaction.user.display_name,
                                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                            )
                        else:
                            error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                        print(error)
                        error_channel = bot.get_channel(ERROR_ID)
                        if error_channel:
                            await error_channel.send(embed=error_embed)
                        for t in TASKS.values():
                            if t.discord_id == interaction.user.id and t.status == "running":
                                t.status = "error"
                                t.end_time = datetime.now(pytz.UTC)
                    else:
                        await asyncio.sleep(60)

            except Exception as error:
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Follow Error {EYES_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred during follow after 10 retry attempts",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'success_count' in locals() and success_count > 0:
                        error_embed.add_field(name="Total Gained", value=success_count, inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    print(error)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                else:
                    await asyncio.sleep(60)

async def extract_duolingo_user_id(jwt_token):
    try:
        parts = jwt_token.split('.')
        if len(parts) != 3:
            raise ValueError("[LOG] Invalid JWT token format")
        
        payload_encoded = parts[1]
        payload_decoded = base64.urlsafe_b64decode(payload_encoded + "==").decode('utf-8')
        payload = json.loads(payload_decoded)

        user_id = payload.get('sub')
        if not user_id:
            raise ValueError("User ID not found in JWT payload")
        
        return user_id

    except Exception as e:
        print(f"[LOG] Error extracting user ID from JWT: {e}")
        return None

class BlockerScanView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_blockerscan"
            )
            select.callback = self.blockerscan_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def blockerscan_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.is_completed = True
            self.stop()
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Blocker Scan",
                description=f"{CHECK_EMOJI} Please wait while we scan for blockers...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)

                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Blocker Scan",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                blocked_user_ids = await get_blocked_users(duo_info)
                if blocked_user_ids is not None and len(blocked_user_ids) > 0:
                    USERS_PER_PAGE = 10
                    user_chunks = [blocked_user_ids[i:i + USERS_PER_PAGE] for i in range(0, len(blocked_user_ids), USERS_PER_PAGE)]

                    for chunk_index, user_chunk in enumerate(user_chunks):
                        current_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Blocker Scan",
                            description=f"{CHECK_EMOJI} You have been blocked by **{len(blocked_user_ids)}** users.\nPage {chunk_index + 1}/{len(user_chunks)}",
                            color=discord.Color.green()
                        )
                        current_embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )

                        for blocked_user_id in user_chunk:
                            current_embed.add_field(
                                name="",
                                value=f"- [{blocked_user_id}](https://www.duolingo.com/u/{blocked_user_id})",
                                inline=False
                            )

                        self.embeds.append(current_embed)
                    
                    self.navigation_view = NavigationView(self.embeds)
                    self.navigation_view.message = await interaction.edit_original_response(embed=self.embeds[0], view=self.navigation_view)
                else:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Blocker Scan",
                        description=f"{CHECK_EMOJI} No users have blocked you!",
                        color=discord.Color.green()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed)
            
        except Exception as e:
            print(f"[LOG] An error occurred in blockerscan_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()
        
class LeaderboardView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        profiles = await db.list_profiles(self.user_id)
        options = []
        for profile in profiles:
            options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))

        if options:
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_leaderboard"
            )
            select.callback = self.leaderboard_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def leaderboard_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            self.is_completed = True
            self.stop()
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Profile not found.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return

            jwt_token = profile["jwt_token"]
            headers = await getheaders(jwt_token, duo_id)

            loading_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Leaderboard",
                description=f"{CHECK_EMOJI} Please wait while we fetch the leaderboard...",
                color=discord.Color.teal()
            )
            loading_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=loading_embed, view=None)
            
            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return
                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)
                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                blocked_users = await get_blocked(duo_info)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Leaderboard",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                url = f"https://duolingo-leaderboards-prod.duolingo.com/leaderboards/7d9f5dd1-8423-491a-91f2-2532052038ce/users/{duo_id}?client_unlocked=true&get_reactions=true&_={int(time.time() * 1000)}"
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        await leaderboard_registration(profile, session, duo_info)
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Leaderboard",
                            description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                            color=discord.Color.green()
                        )
                        embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return
                        
                    json_response = await response.json()
                    if not json_response or not json_response.get('active') or not json_response['active'].get('cohort') or not json_response['active']['cohort'].get('rankings'):
                        await leaderboard_registration(profile, session, duo_info)
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Leaderboard",
                            description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                            color=discord.Color.green()
                        )
                        embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return
                    
                    rankings = json_response.get('active', {}).get('cohort', {}).get('rankings', [])
                    tier = json_response.get('tier', 9)
                    league_names = {
                        0: "Bronze",
                        1: "Silver",
                        2: "Gold",
                        3: "Sapphire",
                        4: "Ruby",
                        5: "Emerald",
                        6: "Amethyst",
                        7: "Pearl",
                        8: "Obsidian",
                        9: "Diamond"
                    }
                    league_name = league_names.get(tier, "Diamond")
                    
                    USERS_PER_PAGE = 5
                    current_embed = None
                    user_chunks = [rankings[i:i + USERS_PER_PAGE] for i in range(0, len(rankings), USERS_PER_PAGE)]

                    for chunk_index, user_chunk in enumerate(user_chunks):
                        current_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Leaderboard",
                            description=f"{CHECK_EMOJI} League: **{league_name}**\n{CHECK_EMOJI} Total Users: **{len(rankings)}**\n{CHECK_EMOJI} Page {chunk_index + 1}/{len(user_chunks)}",
                            color=discord.Color.green()
                        )
                        current_embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )

                        for index, user in enumerate(user_chunk, start=1 + chunk_index * USERS_PER_PAGE):
                            user_id = user.get('user_id')
                            user_info = None
                            if user_id == duo_id:
                                user_info = await getduoinfo(user_id, jwt_token, session)
                            else:
                                user_info = await getduolingoinfo(user_id, session)
                            
                            display_name = user.get('display_name', 'Unknown')
                            score = user.get('score', 0)
                            reactions = user.get('reaction', "NONE")
                            reaction_text = await transform_code(reactions)

                            if user_info:
                                username = user_info.get('username', 'Unknown')
                                profile_link = f"https://www.duolingo.com/profile/{username}"
                                is_moderator = user_info.get('canUseModerationTools', False)
                                has_plus = user_info.get('hasPlus', False)
                                streak = user_info.get('streak', 0)
                                duo_profile = await db.duolingo.find_one({"_id": user_id})
                                used_cheat = False
                                hide = False
                                is_blocked = duo_id in (blocked_users or [])
                                if duo_profile:
                                    used_cheat = True
                                    discord_id = duo_profile.get("discord_id")
                                    discord_profile = await db.discord.find_one({"_id": discord_id})
                                    hide = discord_profile.get("hide", False)

                                user_title = f"[**{display_name}**]({profile_link})"
                                
                                if is_moderator:
                                    user_title += f" (Mod {DUO_MAD_EMOJI})"
                                if has_plus:
                                    user_title += f" {SUPER_EMOJI}"
                                if used_cheat and not hide:
                                    user_title += f" (DuoXPy {MAX_EMOJI})"
                                if is_blocked:
                                    user_title += " (Blocked)"
                                    
                                user_details = (
                                    f"{DIAMOND_TROPHY_EMOJI} #{index} {user_title}\n"
                                    f"{XP_EMOJI} XP: {score:,}\n"
                                    f"{STREAK_EMOJI} Streak: {streak}\n"
                                )
                            else:
                                profile_link = f"https://www.duolingo.com/u/{user_id}"
                                duo_profile = await db.duolingo.find_one({"_id": user_id})
                                used_cheat = False
                                hide = False
                                if duo_profile:
                                    used_cheat = True
                                    discord_id = duo_profile.get("discord_id")
                                    discord_profile = await db.discord.find_one({"_id": discord_id})
                                    hide = discord_profile.get("hide", False)

                                user_title = f"[**{display_name}**]({profile_link})"
                                if used_cheat and not hide:
                                    user_title += f" (DuoXPy {MAX_EMOJI})"

                                user_details = (
                                    f"{DIAMOND_TROPHY_EMOJI} #{index} {user_title}\n"
                                    f"{XP_EMOJI} XP: {score:,}\n"
                                )
                                
                            if reactions != "NONE" and reaction_text != "NONE":
                                user_details += f"{EYES_EMOJI} Reaction: {reaction_text}\n"
                                
                            current_embed.add_field(
                                name=f"#{index}",
                                value=user_details,
                                inline=False
                            )
                            await asyncio.sleep(1)
                        self.embeds.append(current_embed)
                    self.navigation_view = NavigationView(self.embeds)
                    self.navigation_view.message = await interaction.edit_original_response(embed=self.embeds[0], view=self.navigation_view)
        except Exception as e:
            print(f"[LOG] An error occurred in leaderboard_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()

class NavigationView(discord.ui.View):
    def __init__(self, embeds):
        super().__init__(timeout=60)
        self.embeds = embeds
        self.current_page = 0
        self.message = None
        self.update_button_states()

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)

    @discord.ui.button(emoji="⬅️", style=discord.ButtonStyle.secondary)
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self.update_button_states()
            await interaction.edit_original_response(embed=self.embeds[self.current_page], view=self)

    @discord.ui.button(emoji="➡️", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if self.current_page < len(self.embeds) - 1:
            self.current_page += 1
            self.update_button_states()
            await interaction.edit_original_response(embed=self.embeds[self.current_page], view=self)

    def update_button_states(self):
        self.previous_page.disabled = self.current_page == 0
        self.next_page.disabled = self.current_page == len(self.embeds) - 1

class GiveItemView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.item_names = {}
        self.message = None
        self.command_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        options = []
        for profile in self.profiles:
            options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))

        if options:
            select = discord.ui.Select(
                placeholder="Select accounts",
                options=options,
                max_values=len(options),
                min_values=1,
                custom_id="select_accounts_giveitem"
            )
            select.callback = self.accounts_select_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None)

    async def accounts_select_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            selected_ids = interaction.data['values']
            selected_profiles = [p for p in self.profiles if str(p["_id"]) in selected_ids]
            
            items = [
                ("🔥 Streak Freeze (Max-6)", "society_streak_freeze"),
                ("🔄 Streak Repair", "streak_repair"),
                ("💗 Heart Segment (one-by-one)", "heart_segment"),
                ("❤️ Health Refill", "health_refill"),
                ("⚡ XP Boost Stackable", "xp_boost_stackable"),
                ("⚡ General XP Boost", "general_xp_boost"),
                ("⚡ XP Boost x2 15 Mins", "xp_boost_15"),
                ("⚡ XP Boost x2 60 Mins", "xp_boost_60"),
                ("⚡ XP Boost x3 15 Mins", "xp_boost_refill"),
                ("🌅 Early Bird XP Boost", "early_bird_xp_boost"),
                ("🎯 Row Blaster 150", "row_blaster_150"),
                ("🎯 Row Blaster 250", "row_blaster_250"),
            ]

            item_select = discord.ui.Select(
                placeholder="Choose an item...",
                options=[discord.SelectOption(label=item[0], value=item[1], emoji=EYES_EMOJI) for item in items],
                custom_id="select_item"
            )
            self.item_names = {item[1]: item[0] for item in items}
            item_select.callback = lambda i: self.item_select_callback(i, selected_profiles)

            view = discord.ui.View(timeout=60)
            view.add_item(item_select)

            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Get Item",
                description="",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=view)
            self.message = await interaction.original_response()

        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

    async def item_select_callback(self, interaction: discord.Interaction, selected_profiles):
        await interaction.response.defer(ephemeral=True)
        try:
            picked_item_id = interaction.data['values'][0]
            picked_item_name = self.item_names.get(picked_item_id, picked_item_id)
            
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Processing",
                description=f"{CHECK_EMOJI} Please wait while we process your request...",
                color=discord.Color.teal()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

            success_count = 0
            fail_count = 0
            
            async with await get_session(direct=True) as session:
                for profile in selected_profiles:
                    duo_id = profile["_id"]
                    jwt_token = profile["jwt_token"]
                    duo_info = await getduoinfo(duo_id, jwt_token, session)
                    if not duo_info:
                        fail_count += 1
                        await asyncio.sleep(2)
                        continue
                    fromLanguage = duo_info.get('fromLanguage', 'Unknown')
                    learningLanguage = duo_info.get('learningLanguage', 'Unknown')
                    headers = await getheaders(jwt_token, duo_id)

                    if picked_item_id == "xp_boost_refill":
                        inner_body = {
                            "isFree": False,
                            "learningLanguage": learningLanguage,
                            "subscriptionFeatureGroupId": 0,
                            "xpBoostSource": "REFILL",
                            "xpBoostMinutes": 15,
                            "xpBoostMultiplier": 3,
                            "id": picked_item_id
                        }
                        
                        payload = {
                            "includeHeaders": True,
                            "requests": [
                                {
                                    "url": f"/2023-05-23/users/{duo_id}/shop-items",
                                    "extraHeaders": {},
                                    "method": "POST",
                                    "body": json.dumps(inner_body)
                                }
                            ]
                        }
                        
                        url = "https://ios-api-2.duolingo.com/2023-05-23/batch"
                        headers["host"] = "ios-api-2.duolingo.com"
                        headers["x-amzn-trace-id"] = f"User={duo_id}"
                        data = payload
                        
                    else:
                        data = {
                            "itemName": picked_item_id,
                            "isFree": True,
                            "consumed": True,
                            "fromLanguage": fromLanguage,
                            "learningLanguage": learningLanguage
                        }
                        url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/shop-items"
                    try:
                        async with session.post(url, headers=headers, json=data) as response:
                            if response.status == 200:
                                success_count += 1
                            else:
                                fail_count += 1
                    except:
                        fail_count += 1
                    
                    await asyncio.sleep(2)

            if success_count > 0 and fail_count == 0:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Success",
                    description=f"{CHECK_EMOJI} Successfully given **{picked_item_name}** to {success_count} account(s)",
                    color=discord.Color.green()
                )
            elif success_count > 0 and fail_count > 0:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Partial Success",
                    description=f"{CHECK_EMOJI} Given **{picked_item_name}** to {success_count} account(s)\n{FAIL_EMOJI} Failed for {fail_count} account(s)",
                    color=discord.Color.green()
                )
            else:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Failed to give **{picked_item_name}** to all {fail_count} account(s)",
                    color=discord.Color.red()
                )

            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            self.command_completed = True
            self.stop()

        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Command Failure",
                description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            self.command_completed = True
            self.stop()

class SetStatusView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.selected_profile = None
        self.message = None
        self.command_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(int(self.user_id))
        options = []
        for profile in self.profiles:
            options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))

        if options:
            select = discord.ui.Select(
                placeholder="Select accounts",
                options=options,
                max_values=len(options),
                min_values=1,
                custom_id="select_account_setstatus"
            )
            select.callback = self.account_select_callback
            self.add_item(select)

    async def account_select_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.selected_accounts = interaction.data['values']
        await self.show_group_selection(interaction)

    async def show_group_selection(self, interaction: discord.Interaction):
        self.clear_items()
        group_select = discord.ui.Select(
            placeholder="Select a group",
            options=[
                discord.SelectOption(label="Languages Part 1", value="group1", emoji=GLOBE_DUOLINGO_EMOJI),
                discord.SelectOption(label="Languages & Emojis Part 2", value="group2", emoji=QUEST_EMOJI),
                discord.SelectOption(label="Exclusive Statuses", description="Only available for Duolingo IOS", value="group3", emoji=DIAMOND_TROPHY_EMOJI)
            ],
            custom_id="select_group_setstatus"
        )
        group_select.callback = self.group_select_callback
        self.add_item(group_select)

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Set Status",
            description="",
            color=discord.Color.green()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=self)

    async def group_select_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        group = interaction.data['values'][0]
        items = await get_status_items(group)
        
        self.clear_items()
        options = [discord.SelectOption(label=item, emoji=EYES_EMOJI) for item in items]
        select = discord.ui.Select(
            placeholder="Select a status...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="status_select"
        )
        select.callback = self.item_select_callback
        self.add_item(select)

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Set Status",
            description=f"",
            color=discord.Color.green()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=self)

    async def item_select_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Processing",
                description=f"{CHECK_EMOJI} Please wait while we process your request...",
                color=discord.Color.teal()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

            success_count = 0
            fail_count = 0
            pickedItem = interaction.data['values'][0]
            item = await transform_country_code(pickedItem)

            items = [
                "CAT", "POOP", "POPCORN", "DUMPSTER_FIRE", "TROPHY",
                "NONE", "EYES", "FLEX", "ONE_HUNDRED", "POPPER",
                "SUNGLASSES", "ANGRY",
                "YEAR_IN_REVIEW,2023_top1", "YEAR_IN_REVIEW,2023_top3", "YEAR_IN_REVIEW,2023_top5", "YEAR_IN_REVIEW,2023",
                "TROPHY,winner"
            ]

            fixedstatus = item if item in items else f"FLAG,{item}"

            async with await get_session(direct=True) as session:
                for account_id in self.selected_accounts:
                    profile = await db.duolingo.find_one({"_id": int(account_id)})
                    if not profile:
                        fail_count += 1
                        await asyncio.sleep(2)
                        continue

                    jwt_token = profile["jwt_token"]
                    headers = await getheaders(jwt_token, account_id)
                    duo_info = await getduoinfo(account_id, jwt_token, session)
                    if not duo_info:
                        fail_count += 1
                        await asyncio.sleep(2)
                        continue

                    try:
                        leaderboard_url = f"https://duolingo-leaderboards-prod.duolingo.com/leaderboards/7d9f5dd1-8423-491a-91f2-2532052038ce/users/{account_id}?client_unlocked=true&get_reactions=true&_={int(time.time() * 1000)}"
                        async with session.get(leaderboard_url, headers=headers) as response:
                            if response.status != 200:
                                await leaderboard_registration(profile, session, duo_info)
                                fail_count += 1
                                await asyncio.sleep(2)
                                continue

                            leaderboard_data = await response.json()
                            if not leaderboard_data or 'active' not in leaderboard_data:
                                await leaderboard_registration(profile, session, duo_info)
                                fail_count += 1
                                await asyncio.sleep(2)
                                continue

                            active_data = leaderboard_data.get('active')
                            if not active_data or 'cohort' not in active_data:
                                await leaderboard_registration(profile, session, duo_info)
                                fail_count += 1
                                await asyncio.sleep(2)
                                continue
   
                            cohort = active_data['cohort'].get('cohort_id')

                            url = f"https://duolingo-leaderboards-prod.duolingo.com/reactions/{cohort}/users/{account_id}"
                            data2 = {"reaction": fixedstatus}

                            async with session.patch(url, headers=headers, json=data2) as response:
                                if response.status == 200:
                                    success_count += 1
                                else:
                                    fail_count += 1

                    except:
                        fail_count += 1

                    await asyncio.sleep(2)

            if success_count > 0 and fail_count == 0:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Success",
                    description=f"{CHECK_EMOJI} Successfully set status to **{pickedItem}** for {success_count} account(s)",
                    color=discord.Color.green()
                )
            elif success_count > 0 and fail_count > 0:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Partial Success",
                    description=f"{CHECK_EMOJI} Set status for {success_count} account(s)\n{FAIL_EMOJI} Failed for {fail_count} account(s)",
                    color=discord.Color.green()
                )
            else:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Failed to set status for all {fail_count} account(s)",
                    color=discord.Color.red()
                )

            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Command Failure", 
                description=f"{FAIL_EMOJI} An unexpected error occurred",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            print(e)
        finally:
            self.command_completed = True
            self.stop()

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Set Status",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()

async def transform_country_code(input_str):
    language_mapping = {
        "Arabic 🇸🇦": "ar",
        "Catalan 🇦🇩": "ca", 
        "Czech 🇨ZE": "cs",
        "Welsh 🏴": "cy",
        "German 🇩🇪": "de",
        "Greek 🇬🇷": "el",
        "English 🇬🇧 / 🇺🇸": "en",
        "Esperanto 🌍": "eo",
        "Spanish 🇪🇸": "es",
        "Finnish 🇫🇮": "fi",
        "French 🇫🇷": "fr",
        "Irish 🇮🇪": "ga",
        "Scottish Gaelic 🏴": "gd",
        "Guarani 🇵🇾": "gn",
        "Hebrew 🇮🇱": "he",
        "Hindi 🇮🇳": "hi",
        "Haitian Creole 🇭🇹": "ht",
        "Hungarian 🇭🇺": "hu",
        "Hiri Motu 🇵🇬": "hv",
        "Hawaiian 🏴": "hw",
        "Italian 🇮🇹": "it",
        "Japanese 🇯🇵": "ja",
        "Korean 🇰🇷": "ko",
        "Latin 🇻🇦": "la",
        "Navajo 🏴": "nv",
        "Portuguese 🇵🇹 / 🇧🇷": "pt",
        "Romanian 🇷🇴": "ro",
        "Russian 🇷🇺": "ru",
        "Swedish 🇸🇪": "sv",
        "Swahili 🇹🇿 / 🇰🇪": "sw",
        "Turkish 🇹🇷": "tr",
        "Ukrainian 🇺🇦": "uk",
        "Vietnamese 🇻🇳": "vi",
        "Yiddish 🏴": "yi",
        "Zulu 🇿🇦": "zu",
        "Cat 🐱": "CAT",
        "Poop 💩": "POOP",
        "Popcorn 🍿": "POPCORN", 
        "Dumpster Fire 🔥": "DUMPSTER_FIRE",
        "Trophy 🏆": "TROPHY",
        "Eyes 👀": "EYES",
        "Flex 💪": "FLEX",
        "One Hundred 💯": "ONE_HUNDRED",
        "Popper 🎉": "POPPER",
        "Sunglasses 😎": "SUNGLASSES",
        "Angry 😠": "ANGRY",
        "Diamond Trophy 💎": "TROPHY,winner",
        "2023 Event Top 1 👑": "YEAR_IN_REVIEW,2023_top1",
        "2023 Event Top 3 ⭐": "YEAR_IN_REVIEW,2023_top3",
        "2023 Event Top 5 🌟": "YEAR_IN_REVIEW,2023_top5",
        "2023 Event Everyone 🎊": "YEAR_IN_REVIEW,2023"
    }
    return language_mapping.get(input_str, 'unknown')

async def transform_code(input_str):
    code_mapping = {
        "ar": "Arabic 🇸🇦",
        "ca": "Catalan 🇦🇩",
        "cs": "Czech 🇨ZE",
        "cy": "Welsh 🏴",
        "de": "German 🇩🇪",
        "el": "Greek 🇬🇷",
        "en": "English 🇬🇧 / 🇺🇸",
        "eo": "Esperanto 🌍",
        "es": "Spanish 🇪🇸",
        "fi": "Finnish 🇫🇮",
        "fr": "French 🇫🇷",
        "ga": "Irish 🇮🇪",
        "gd": "Scottish Gaelic 🏴",
        "gn": "Guarani 🇵🇾",
        "he": "Hebrew 🇮🇱",
        "hi": "Hindi 🇮🇳",
        "ht": "Haitian Creole 🇭🇹",
        "hu": "Hungarian 🇭🇺",
        "hv": "Hiri Motu 🇵🇬",
        "hw": "Hawaiian 🏴",
        "it": "Italian 🇮🇹",
        "ja": "Japanese 🇯🇵",
        "ko": "Korean 🇰🇷",
        "la": "Latin 🇻🇦",
        "nv": "Navajo 🏴",
        "pt": "Portuguese 🇵🇹 / 🇧🇷",
        "ro": "Romanian 🇷🇴",
        "ru": "Russian 🇷🇺",
        "sv": "Swedish 🇸🇪",
        "sw": "Swahili 🇹🇿 / 🇰🇪",
        "tr": "Turkish 🇹🇷",
        "uk": "Ukrainian 🇺🇦",
        "vi": "Vietnamese 🇻🇳",
        "yi": "Yiddish 🏴",
        "zu": "Zulu 🇿🇦",
        "CAT": "Cat 🐱",
        "POOP": "Poop 💩",
        "POPCORN": "Popcorn 🍿",
        "DUMPSTER_FIRE": "Dumpster Fire 🔥",
        "TROPHY": "Trophy 🏆",
        "EYES": "Eyes 👀",
        "FLEX": "Flex 💪",
        "ONE_HUNDRED": "One Hundred 💯",
        "POPPER": "Popper 🎉",
        "SUNGLASSES": "Sunglasses 😎",
        "ANGRY": "Angry 😠",
        "TROPHY,winner": "Diamond Trophy 💎",
        "YEAR_IN_REVIEW,2023_top1": "2023 Event Top 1 👑",
        "YEAR_IN_REVIEW,2023_top3": "2023 Event Top 3 ⭐",
        "YEAR_IN_REVIEW,2023_top5": "2023 Event Top 5 🌟",
        "YEAR_IN_REVIEW,2023": "2023 Event Everyone 🎊"
    }
    return code_mapping.get(input_str, 'NONE')

async def get_status_items(group: str):
    if group == "group1":
        return [
            "Arabic 🇸🇦", "Catalan 🇦🇩", "Czech 🇨ZE", "Welsh 🏴", "German 🇩🇪",
            "Greek 🇬🇷", "English 🇬🇧 / 🇺🇸", "Esperanto 🌍", "Spanish 🇪🇸", "Finnish 🇫🇮",
            "French 🇫🇷", "Irish 🇮🇪", "Scottish Gaelic 🏴", "Guarani 🇵🇾", "Hebrew 🇮🇱",
            "Hindi 🇮🇳", "Haitian Creole 🇭🇹", "Hungarian 🇭🇺", "Hiri Motu 🇵🇬", "Hawaiian 🏴",
            "Italian 🇮🇹", "Japanese 🇯🇵", "Korean 🇰🇷", "Latin 🇻🇦", "Navajo 🏴"
        ]
    elif group == "group2":
        return [
            "Portuguese 🇵🇹 / 🇧🇷", "Romanian 🇷🇴", "Russian 🇷🇺", "Swedish 🇸🇪", "Swahili 🇹🇿 / 🇰🇪",
            "Turkish 🇹🇷", "Ukrainian 🇺🇦", "Vietnamese 🇻🇳", "Yiddish 🏴", "Zulu 🇿🇦",
            "Cat 🐱", "Poop 💩", "Popcorn 🍿", "Dumpster Fire 🔥", "Trophy 🏆",
            "Eyes 👀", "One Hundred 💯", "Popper 🎉", "Sunglasses 😎", "Angry 😠",
            "Diamond Trophy 💎", "Flex 💪"
        ]
    else:
        return [
            "2023 Event Top 1 👑", "2023 Event Top 3 ⭐",
            "2023 Event Top 5 🌟", "2023 Event Everyone 🎊",
        ]

async def get_blocked_users(duo_info: dict):
    if duo_info:
        return duo_info.get("blockerUserIds", [])
    return None

async def getimageurl(duo_info: dict = None):
    if duo_info:
        url = f"{duo_info['picture']}/xlarge"
        return url
    return None

async def process_streak_farm(interaction: discord.Interaction, day_repair: int, process_data: dict, profile: dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id")
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    timezone_name = profile.get("timezone", "Asia/Saigon")
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Streak {STREAK_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Streak {STREAK_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "streak" and t.status == "running":
                task_obj = t
                break
        if task_obj:
            task_obj.progress = 0
            task_obj.estimated_time_left = None
        embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Streak {STREAK_EMOJI}",
                description=f"{CHECK_EMOJI} Task started, please use /task to check progress",
                color=discord.Color.teal()
        )
        if hide:
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
        else:
            embed.set_author(name=duo_username, icon_url=duo_avatar)
        await interaction.edit_original_response(embed=embed)
        while retry_attempts < 10:
            try:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if not duo_info:
                    raise e
                fromLanguage = duo_info.get('fromLanguage', 'Unknown')
                learningLanguage = duo_info.get('learningLanguage', 'Unknown')
                streak_data = duo_info.get('streakData', {})
                current_streak = streak_data.get('currentStreak', {})
                
                user_tz = pytz.timezone(timezone_name)
                now = datetime.now(user_tz)
                
                if not current_streak:
                    streak_start_date = now
                else:
                    try:
                        streak_start_date = datetime.strptime(current_streak.get('startDate'), "%Y-%m-%d")
                    except:
                        if task_obj:
                            if not task_obj.status == "cancelled":
                                task_obj.status = "completed"
                            task_obj.end_time = datetime.now(pytz.UTC)
                        success_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Streak Success {STREAK_EMOJI}",
                            description=f"{CHECK_EMOJI} Successfully reached maximum streak days!",
                            color=discord.Color.gold(),
                            timestamp=datetime.now()
                        )
                        if hide:
                            success_embed.set_author(
                                name=interaction.user.display_name,
                                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                            )
                        else:
                            success_embed.set_author(name=duo_username, icon_url=duo_avatar)
                        if success_channel:
                            await success_channel.send(embed=success_embed)
                        return

                total_xp = 0
                start_time = time.time()
                day_count = 0

                target_date = datetime(1, 1, 1) if day_repair == 0 else None
                if task_obj:
                    task_obj.total = (streak_start_date - target_date).days if day_repair == 0 else day_repair
                while True:
                    if task_obj and task_obj.status != "running":
                        break
                    if day_repair > 0 and day_count >= day_repair:
                        break

                    if task_obj:
                        task_obj.progress = day_count
                        elapsed_time = time.time() - start_time
                        if day_repair > 0:
                            estimated_total_time = (elapsed_time / (day_count + 1)) * day_repair
                            estimated_remaining_time = estimated_total_time - elapsed_time
                            task_obj.estimated_time_left = estimated_remaining_time

                    try:
                        simulated_day = streak_start_date - timedelta(days=day_count)
                        
                        if day_repair == 0 and simulated_day <= target_date or simulated_day.year <= 0:
                            break
                            
                    except:
                        break

                    session_payload = {
                        "challengeTypes": [
                            "assist", "characterIntro", "characterMatch", "characterPuzzle",
                            "characterSelect", "characterTrace", "characterWrite",
                            "completeReverseTranslation", "definition", "dialogue",
                            "extendedMatch", "extendedListenMatch", "form", "freeResponse",
                            "gapFill", "judge", "listen", "listenComplete", "listenMatch",
                            "match", "name", "listenComprehension", "listenIsolation",
                            "listenSpeak", "listenTap", "orderTapComplete", "partialListen",
                            "partialReverseTranslate", "patternTapComplete", "radioBinary",
                            "radioImageSelect", "radioListenMatch", "radioListenRecognize",
                            "radioSelect", "readComprehension", "reverseAssist",
                            "sameDifferent", "select", "selectPronunciation",
                            "selectTranscription", "svgPuzzle", "syllableTap",
                            "syllableListenTap", "speak", "tapCloze", "tapClozeTable",
                            "tapComplete", "tapCompleteTable", "tapDescribe", "translate",
                            "transliterate", "transliterationAssist", "typeCloze",
                            "typeClozeTable", "typeComplete", "typeCompleteTable",
                            "writeComprehension"
                        ],
                        "fromLanguage": fromLanguage,
                        "isFinalLevel": False,
                        "isV2": True,
                        "juicy": True,
                        "learningLanguage": learningLanguage,
                        "smartTipsVersion": 2,
                        "type": "GLOBAL_PRACTICE"
                    }

                    retry_count = 0
                    while retry_count < 10:
                        async with session.post("https://www.duolingo.com/2017-06-30/sessions", headers=headers, json=session_payload) as response:
                            if response.status == 200:
                                session_data = await response.json()
                                retry_count = 0
                                break
                            else:
                                retry_count += 1
                                if retry_count < 10:
                                    await asyncio.sleep(60)
                                else:
                                    raise Exception(f"Failed to create session after 10 attempts. Status: {response.status}")

                    if 'id' not in session_data:
                        raise Exception("Session ID not found in response")

                    start_timestamp = int((simulated_day - timedelta(seconds=1)).timestamp())
                    end_timestamp = int(simulated_day.timestamp())
                    
                    update_payload = {
                        **session_data,
                        "heartsLeft": 5,
                        "startTime": start_timestamp,
                        "endTime": end_timestamp,
                        "enableBonusPoints": False,
                        "failed": False,
                        "maxInLessonStreak": 9,
                        "shouldLearnThings": True
                    }

                    retry_count = 0
                    while retry_count < 10:
                        async with session.put(f"https://www.duolingo.com/2017-06-30/sessions/{session_data['id']}", headers=headers, json=update_payload) as response:
                            if response.status == 200:
                                update_response = await response.json()
                                retry_count = 0
                                break
                            else:
                                retry_count += 1
                                if retry_count < 10:
                                    await asyncio.sleep(60)
                                else:
                                    raise Exception(f"Failed to update session after 10 attempts. Status: {response.status}")
                    
                    day_count += 1

                total_time = time.time() - start_time
                hours, remainder = divmod(int(total_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h " if hours > 0 else ""
                time_str += f"{minutes}m " if minutes > 0 else ""
                time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                if task_obj:
                    task_obj.progress = day_count
                    task_obj.estimated_time_left = 0
                    task_obj.end_time = datetime.now(pytz.UTC)
                    if not task_obj.status == "cancelled":
                        task_obj.status = "completed"

                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Streak Success {STREAK_EMOJI}",
                    description="",
                    color=discord.Color.orange(),
                    timestamp=datetime.now()
                )
                final_embed.add_field(name="Days", value=day_count, inline=True)
                final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                if hide:
                    final_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                if success_channel:
                    await success_channel.send(embed=final_embed)
                break

            except Exception as error:                
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Streak Error {STREAK_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred during streak farm after 10 retry attempts",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'day_count' in locals() and day_count > 0:
                        error_embed.add_field(name="Days", value=f"{day_count}/{day_repair if day_repair > 0 else 'inf'}", inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    error_channel = bot.get_channel(ERROR_ID)
                    print(error)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                else:
                    await asyncio.sleep(60)

class StopView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = str(user_id)
        self.message = None
        self.command_completed = False

    async def setup(self):
        user_id = int(self.user_id)
        def is_active(task_type):
            return any(
                t.discord_id == user_id and t.task_type.lower() == task_type.lower() and t.status == "running"
                for t in TASKS.values()
            )
        options = [
            discord.SelectOption(
                label="Stop All Tasks",
                description="Stop all active tasks",
                value="all",
                emoji=FAIL_EMOJI
            ),
            discord.SelectOption(
                label="XP",
                description="Active" if is_active("xp") else "Inactive",
                value="XP",
                emoji=XP_EMOJI
            ),
            discord.SelectOption(
                label="Gem",
                description="Active" if is_active("gem") else "Inactive",
                value="Gem",
                emoji=GEM_EMOJI
            ),
            discord.SelectOption(
                label="Streak",
                description="Active" if is_active("streak") else "Inactive",
                value="Streak",
                emoji=STREAK_EMOJI
            ),
            discord.SelectOption(
                label="League",
                description="Active" if is_active("league") else "Inactive",
                value="League",
                emoji=DIAMOND_TROPHY_EMOJI
            ),
            discord.SelectOption(
                label="Follow",
                description="Active" if is_active("follow") else "Inactive",
                value="Follow",
                emoji=EYES_EMOJI
            ),
            discord.SelectOption(
                label="Unfollow",
                description="Active" if is_active("unfollow") else "Inactive",
                value="Unfollow",
                emoji=TRASH_EMOJI
            ),
            discord.SelectOption(
                label="Block",
                description="Active" if is_active("block") else "Inactive",
                value="Block",
                emoji=DUO_MAD_EMOJI
            ),
            discord.SelectOption(
                label="Unblock",
                description="Active" if is_active("unblock") else "Inactive",
                value="Unblock",
                emoji=NERD_EMOJI
            ),
            discord.SelectOption(
                label="League Saver",
                description="Active" if is_active("leaguesaver") else "Inactive", 
                value="LeagueSaver",
                emoji=DIAMOND_TROPHY_EMOJI
            ),
            discord.SelectOption(
                label="Quest Saver",
                description="Active" if is_active("questsaver") else "Inactive",
                value="QuestSaver", 
                emoji=QUEST_EMOJI
            ),
            discord.SelectOption(
                label="Streak Saver",
                description="Active" if is_active("streaksaver") else "Inactive",
                value="StreakSaver",
                emoji=STREAK_EMOJI
            )
        ]
        select = discord.ui.Select(
            placeholder="Select task to stop",
            options=options,
            custom_id="select_stop"
        )
        select.callback = self.stop_callback
        self.add_item(select)

    async def stop_callback(self, interaction: discord.Interaction):
        try:
            self.command_completed = True
            await interaction.response.defer(ephemeral=True)
            user_id = str(interaction.user.id)
            task_type = interaction.data['values'][0]
            stopped_tasks = []

            def stop_tasks_for_type(type_name):
                found = False
                for t in TASKS.values():
                    if str(t.discord_id) == user_id and (type_name == "all" or t.task_type.lower() == type_name.lower()) and t.status == "running":
                        t.status = "cancelled"
                        t.end_time = datetime.now(pytz.UTC)
                        found = True
                return found

            if task_type == "all":
                any_stopped = False
                for type_name in ["xp", "gem", "streak", "follow", "unfollow", "block", "unblock", "league", "leaguesaver", "questsaver", "streaksaver"]:
                    if stop_tasks_for_type(type_name):
                        stopped_tasks.append(type_name.capitalize())
                        any_stopped = True
                if not any_stopped:
                    stopped_tasks = []
            else:
                if stop_tasks_for_type(task_type):
                    stopped_tasks.append(task_type)

            if stopped_tasks:
                stopped_tasks_str = ", ".join(stopped_tasks)
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Tasks Stopped",
                    description=f"{CHECK_EMOJI} The following tasks have been stopped: **{stopped_tasks_str}**",
                    color=discord.Color.green()
                )
            else:
                task_type_str = "tasks" if task_type == "all" else f"**{task_type}** task"
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: No Active Tasks",
                    description=f"{FAIL_EMOJI} You don't have any active {task_type_str} to stop.",
                    color=discord.Color.red()
                )

            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.followup.edit_message(message_id=self.message.id, embed=embed, view=None)
        except Exception as e:
            print(f"[LOG] Error in stop_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while stopping tasks. Please try again.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.followup.edit_message(message_id=self.message.id, embed=error_embed, view=None)

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
                await self.message.edit(embed=embed, view=None)
        self.stop()

@tasks.loop(minutes=6)
async def user_count_channel():
    try:
        user_count = await db.discord.count_documents({})
        channel = bot.get_channel(USER_COUNT)
        if channel:
            await channel.edit(name=f"👤┃{await transform_to_fancy_font(f'Users: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {USER_COUNT} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the user count channel: {e}")

@tasks.loop(minutes=6)
async def xp_farmers_channel():
    try:
        user_count = len(set(t.discord_id for t in TASKS.values() if t.task_type == "xp" and t.status == "running"))
        channel = bot.get_channel(XP)
        if channel:
            await channel.edit(name=f"⚡┃{await transform_to_fancy_font(f'Farmers: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {XP} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the XP farmers channel: {e}")

@tasks.loop(minutes=6)
async def streak_farmers_channel():
    try:
        user_count = len(set(t.discord_id for t in TASKS.values() if t.task_type == "streak" and t.status == "running"))
        channel = bot.get_channel(STREAK_BUFF)
        if channel:
            await channel.edit(name=f"🔥┃{await transform_to_fancy_font(f'Farmers: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {STREAK_BUFF} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the streak farmers channel: {e}")


@tasks.loop(minutes=6)
async def gem_farmers_channel():
    try:
        user_count = len(set(t.discord_id for t in TASKS.values() if t.task_type == "gem" and t.status == "running"))
        channel = bot.get_channel(GEM)
        if channel:
            await channel.edit(name=f"💎┃{await transform_to_fancy_font(f'Farmers: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {GEM} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the gem farmers channel: {e}")


@tasks.loop(minutes=6)
async def streak_savers_channel():
    try:
        user_count = await db.discord.count_documents({"streaksaver": True})
        channel = bot.get_channel(STREAK_SAVER)
        if channel:
            await channel.edit(name=f"🔥┃{await transform_to_fancy_font(f'Savers: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {STREAK_SAVER} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the streak savers channel: {e}")

@tasks.loop(minutes=6)
async def league_savers_channel():
    try:
        user_count = await db.discord.count_documents({"autoleague.active": True})
        channel = bot.get_channel(LEAGUE)
        if channel:
            await channel.edit(name=f"🏆┃{await transform_to_fancy_font(f'Savers: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {LEAGUE} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the league savers channel: {e}")

@tasks.loop(minutes=6)
async def league_farmers_channel():
    try:
        user_count = len(set(t.discord_id for t in TASKS.values() if t.task_type == "league" and t.status == "running"))
        channel = bot.get_channel(LEAGUE_FARMERS)
        if channel:
            await channel.edit(name=f"🏆┃{await transform_to_fancy_font(f'Farmers: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {LEAGUE_FARMERS} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the league farmers channel: {e}")

@tasks.loop(minutes=6)
async def duo_accounts_channel():
    try:
        user_count = await db.duolingo.count_documents({})
        channel = bot.get_channel(DUO_ACCOUNT)
        if channel:
            await channel.edit(name=f"🦉┃{await transform_to_fancy_font(f'Accounts: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {DUO_ACCOUNT} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the duo accounts channel: {e}")

@tasks.loop(minutes=6)
async def follow_accounts_channel():
    try:
        user_count = await db.follow.count_documents({})
        channel = bot.get_channel(FOLLOW_ACCOUNT)
        if channel:
            await channel.edit(name=f"👀┃{await transform_to_fancy_font(f'Accounts: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {FOLLOW_ACCOUNT} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the duo accounts channel: {e}")

@tasks.loop(minutes=6)
async def quest_savers_channel():
    try:
        user_count = await db.discord.count_documents({"questsaver": True})
        channel = bot.get_channel(QUEST_SAVER)
        if channel:
            await channel.edit(name=f"🎯┃{await transform_to_fancy_font(f'Savers: {user_count}')}")
        else:
            print(f"[LOG] Channel with ID {QUEST_SAVER} not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the quest savers channel: {e}")

@tasks.loop(minutes=6)
async def bot_status_channel():
    try:
        current_status = f"{'🟢' if not BOT_STOPPED else '🔴'}┃{await transform_to_fancy_font(f'v{VERSION}')}"
        channel = bot.get_channel(STATUS)
        if channel:
            await channel.edit(name=current_status)
        else:
            print("[LOG] Status channel not found.")
    except Exception as e:
        print(f"[LOG] An error occurred while updating the bot status channel: {e}")

async def transform_to_fancy_font(text: str) -> str:
    return text.translate(str.maketrans(
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@#$%^&*()_+-=[]{}|;:,.<>?/~`!\'" ',
        '𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵@#$%^&*()_+-=[]{}|;:,.<>?/~`!\'" '
    ))

class HeartRemovalView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.selected_profile = None
        self.message = None
        self.command_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(int(self.user_id))
        options = []
        for profile in self.profiles:
            options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))

        if options:
            account_select = discord.ui.Select(
                placeholder="Select accounts",
                options=options,
                max_values=len(options),
                min_values=1,
                custom_id="select_account_heart_removal"
            )
            account_select.callback = self.account_callback
            self.add_item(account_select)

    async def account_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        selected_profiles = [p for p in self.profiles if str(p["_id"]) in interaction.data['values']]
        
        heart_select = discord.ui.Select(
            placeholder="Select number of hearts to remove",
            options=[
                discord.SelectOption(label=f"{i} Heart{'s' if i > 1 else ''}", value=str(i), emoji=FAIL_EMOJI)
                for i in range(1, 6)
            ],
            custom_id="select_hearts"
        )
        heart_select.callback = lambda i: self.heart_callback(i, selected_profiles)
        self.clear_items()
        self.add_item(heart_select)
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Heart Removal",
            description=f"",
            color=discord.Color.green()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=self)

    async def heart_callback(self, interaction: discord.Interaction, selected_profiles):
        await interaction.response.defer()
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Heart Removal",
            description=f"{CHECK_EMOJI} Please wait while we remove the hearts...",
            color=discord.Color.teal()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)

        self.command_completed = True
        self.stop()
        num_hearts = int(interaction.data['values'][0])
        success_count = 0
        fail_count = 0

        async with await get_session(direct=True) as session:
            for profile in selected_profiles:
                jwt_token = profile["jwt_token"]
                duo_id = str(profile["_id"])
                headers = await getheaders(jwt_token, duo_id)
                url = f"https://duolingo.com/2017-06-30/users/{duo_id}/remove-heart"
                
                for _ in range(num_hearts):
                    try:
                        async with session.put(url, headers=headers) as response:
                            if response.status == 200:
                                success_count += 1
                            else:
                                fail_count += 1
                                print(f"[LOG] Failed to remove heart for user {duo_id}. Status code: {response.status}")
                    except Exception as e:
                        fail_count += 1
                        print(f"[LOG] Error removing heart for user {duo_id}: {e}")
                    await asyncio.sleep(1)

            if success_count > 0 and fail_count == 0:
                status = "Success"
                color = discord.Color.green()
                description = f"{CHECK_EMOJI} Successfully removed {success_count} hearts across {len(selected_profiles)} account(s)"
            elif success_count > 0:
                status = "Partial Success"
                color = discord.Color.orange() 
                description = f"{CHECK_EMOJI} Removed {success_count} hearts\n{FAIL_EMOJI} Failed to remove {fail_count} hearts"
            else:
                status = "Failed"
                color = discord.Color.red()
                description = f"{FAIL_EMOJI} Failed to remove any hearts"

            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Heart Removal {status}",
                description=description,
                color=color
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

    async def on_timeout(self):
        if self.message and not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()

class QuestView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.message = None
        self.command_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = [discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI) for profile in self.profiles]
            select = discord.ui.Select(
                placeholder="Choose account",
                options=options,
                max_values=1,
                min_values=1,
                custom_id="select_account_quest"
            )
            select.callback = self.select_callback
            self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.command_completed = True
        self.stop()
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Complete Quest {QUEST_EMOJI}",
            description=f"{CHECK_EMOJI} Processing quest completion...",
            color=discord.Color.teal()
        )
        embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url)
        await interaction.edit_original_response(embed=embed, view=None)
        
        selected_profile = next((p for p in self.profiles if str(p["_id"]) == interaction.data['values'][0]), None)
        if not selected_profile:
            return

        jwt_token = selected_profile["jwt_token"]
        duo_id = selected_profile["_id"]
        timezone_str = selected_profile.get("timezone", "Asia/Saigon")
        duo_username = None
        duo_avatar = None

        async with await get_session() as session:
            try:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if not duo_info:
                    raise Exception("Failed to fetch account info")
                
                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)
                
                headers = await getheaders(jwt_token, duo_id)
                
                schema_url = "https://goals-api.duolingo.com/schema?ui_language=en"
                async with session.get(schema_url, headers=headers) as schema_response:
                    if schema_response.status != 200:
                        raise Exception("Failed to get schema")

                    schema_data = await schema_response.json()
                    seen_metrics = {}
                    unique_metrics = []
                    
                    for goal in schema_data.get("goals", []):
                        metric = goal.get("metric")
                        if metric and metric not in seen_metrics:
                            seen_metrics[metric] = True
                            unique_metrics.append(metric)

                try:
                    user_tz = pytz.timezone(timezone_str)
                except pytz.exceptions.UnknownTimeZoneError:
                    user_tz = pytz.timezone("Asia/Saigon")

                current_date = datetime.now(user_tz)
                current_day = current_date.day
                current_year = current_date.year

                end_date = datetime(2021, 1, current_day, tzinfo=user_tz)
                start_date = datetime(current_year, 12, current_day, tzinfo=user_tz)

                dates_to_process = []
                temp_date = start_date
                            
                while temp_date >= end_date:
                    _, last_day = calendar.monthrange(temp_date.year, temp_date.month)
                    actual_day = min(current_day, last_day)
                    temp_date = temp_date.replace(day=actual_day)
                    
                    dates_to_process.append(temp_date)
                    temp_date = temp_date - relativedelta(months=1)

                metric_updates = [{"metric": metric, "quantity": 2000} for metric in unique_metrics]
                for target_date in dates_to_process:
                    formatted_time = target_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    json_data = {
                        "metric_updates": metric_updates,
                        "timestamp": formatted_time,
                        "timezone": timezone_str
                    }

                    url = f"https://goals-api.duolingo.com/users/{duo_id}/progress/batch"
                    async with session.post(url, headers=headers, json=json_data):
                        pass

                    await asyncio.sleep(1)

                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Complete Quest {QUEST_EMOJI}",
                    description=f"{CHECK_EMOJI} Quest completion process finished successfully",
                    color=discord.Color.green()
                )

            except Exception as e:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Failed to complete quests.",
                    color=discord.Color.red()
                )

            embed.set_author(name=duo_username, icon_url=duo_avatar)
                
        await interaction.edit_original_response(embed=embed, view=None)
        self.stop()

    async def on_timeout(self):
        if not self.command_completed and self.message:
            timeout_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            timeout_embed.set_author(name=self.message.interaction_metadata.user.display_name, icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url)
            await self.message.edit(embed=timeout_embed, view=None)
        self.stop()

async def is_premium_user(user: discord.User) -> bool:
    guild = bot.get_guild(SERVER)
    member = guild.get_member(user.id)
    if member and member.premium_since is not None:
        return True
        
    user_data = await db.discord.find_one({"_id": user.id})
    if not user_data:
        return False
        
    premium = user_data.get("premium", {})
    return premium.get("active", False) and premium.get("type") in ["monthly", "lifetime", "free", "ultra"]

async def is_premium_free_user(user: discord.User) -> bool:
    user_data = await db.discord.find_one({"_id": user.id})
    if not user_data:
        return False
    premium = user_data.get("premium", {})
    return premium.get("active", False) and premium.get("type") == "free"

async def is_ultra_user(user: discord.User) -> bool:
    user_data = await db.discord.find_one({"_id": user.id})
    if not user_data:
        return False
    premium = user_data.get("premium", {})
    return premium.get("active", False) and premium.get("type") == "ultra"

async def check_and_set_farm_status(interaction: discord.Interaction, farm_type: str, message_id: None) -> bool:
    if any(t.discord_id == interaction.user.id and t.task_type.lower() == farm_type.lower() and t.status == "running" for t in TASKS.values()):
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Task Already Running",
            description=f"{FAIL_EMOJI} You already have an active task: **{farm_type.capitalize()}**. Use **/stop** to stop it.",
            color=discord.Color.red()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)
        return False
    if any(t.discord_id == interaction.user.id and t.status == "running" for t in TASKS.values()):
        current_task = next(t.task_type.capitalize() for t in TASKS.values() 
                          if t.discord_id == interaction.user.id and t.status == "running")
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Task Already Running", 
            description=f"{FAIL_EMOJI} You already have an active task: **{current_task}**. Use **/stop** to stop it.",
            color=discord.Color.red()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)
        return False

    return True

class ToggleHideView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        if discord_profile:
            current_status = discord_profile.get("hide", False)
            
            options = [
                discord.SelectOption(
                    label="Enable Hide" if not current_status else "Disable Hide",
                    description="Status: " + ("Enabled" if current_status else "Disabled"),
                    value="toggle",
                    emoji=CHECK_EMOJI if not current_status else FAIL_EMOJI
                )
            ]

            select = discord.ui.Select(
                placeholder="Configure Hide",
                options=options,
                custom_id="select_hide"
            )
            select.callback = self.hide_callback
            self.add_item(select)
    
    async def hide_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.command_completed = True

        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)
        
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        current_status = discord_profile.get("hide", False) if discord_profile else False
        new_status = not current_status
        
        await db.discord.update_one(
            {"_id": self.user_id},
            {"$set": {"hide": new_status}},
            upsert=True
        )
        
        status_text = "enabled" if new_status else "disabled"
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Hide {EYES_EMOJI}",
            description=f"{CHECK_EMOJI} Hide has been **{status_text}**.",
            color=discord.Color.green() if new_status else discord.Color.red()
        )
        
        embed.set_author(
            name=interaction.user.display_name, 
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)
        self.stop()

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()

async def process_massunfollow(interaction: discord.Interaction, process_data: dict, profile: dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id")
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Unfollow {TRASH_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Unfollow {TRASH_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "unfollow" and t.status == "running":
                task_obj = t
                break

        while retry_attempts < 10:
            try:
                url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings"
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        print(f"[LOG] Failed to get privacy status for user {duo_id}")
                        return
                    data = await response.json()
                    privacy_settings = data.get('privacySettings', [])
                    social_setting = next((setting for setting in privacy_settings if setting['id'] == 'disable_social'), None)
                    was_private = social_setting['enabled'] if social_setting else False

                if was_private:
                    url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings?fields=privacySettings"
                    payload = {"DISABLE_SOCIAL": False}
                    async with session.patch(url, headers=headers, json=payload) as response:
                        if response.status != 200:
                            print(f"[LOG] Failed to set account to public for user {duo_id}")
                            return
                    await asyncio.sleep(2)

                following_url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/following?viewerId={duo_id}"
                async with session.get(following_url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        following = data.get('following', {})
                        total_following = following.get('totalUsers', 0)
                        following_list = following.get('users', [])

                if not following_list:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: No Following",
                        description=f"{CHECK_EMOJI} You are not following any users.",
                        color=discord.Color.green()
                    )
                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed)
                    return

                if task_obj:
                    task_obj.progress = 0
                    task_obj.total = len(following_list)
                    task_obj.estimated_time_left = None

                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Unfollow {TRASH_EMOJI}",
                    description=f"{CHECK_EMOJI} Task started! Use `/task` to check progress.",
                    color=discord.Color.teal()
                )
                if hide:
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                await interaction.edit_original_response(embed=embed)

                success_count = 0
                start_time = time.time()

                for i, user in enumerate(following_list, start=1):
                    if task_obj and task_obj.status != "running":
                        break
                    unfollow_url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/follow/{user['userId']}"
                    async with session.delete(unfollow_url, headers=headers) as response:
                        if response.status == 200:
                            success_count += 1

                    if task_obj:
                        elapsed_time = time.time() - start_time
                        task_obj.progress = i
                        task_obj.estimated_time_left = None
                        if i > 0:
                            estimated_total_time = (elapsed_time / i) * len(following_list)
                            estimated_remaining_time = estimated_total_time - elapsed_time
                            task_obj.estimated_time_left = estimated_remaining_time
                        task_obj.gained = success_count

                    await asyncio.sleep(1)

                total_time = time.time() - start_time
                hours, remainder = divmod(int(total_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h " if hours > 0 else ""
                time_str += f"{minutes}m " if minutes > 0 else ""
                time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                if task_obj:
                    task_obj.progress = len(following_list)
                    task_obj.estimated_time_left = 0
                    task_obj.gained = success_count
                    task_obj.end_time = datetime.now(pytz.UTC)
                    if not task_obj.status == "cancelled":
                        task_obj.status = "completed"

                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Unfollow Success {TRASH_EMOJI}",
                    description=f"",
                    color=discord.Color.green(),
                    timestamp=datetime.now()
                )
                final_embed.add_field(name="Total Unfollowed", value=f"{success_count}/{len(following_list)}", inline=True)
                final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                if hide:
                    final_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                if success_channel:
                    await success_channel.send(embed=final_embed)

                if was_private:
                    url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings?fields=privacySettings"
                    payload = {"DISABLE_SOCIAL": True}
                    async with session.patch(url, headers=headers, json=payload) as response:
                        if response.status != 200:
                            print(f"[LOG] Failed to restore private settings for user {duo_id}")
                break

            except Exception as error:
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Unfollow Error {TRASH_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred during unfollow after 10 retry attempts",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'success_count' in locals() and success_count > 0:
                        error_embed.add_field(name="Total Unfollowed", value=f"{success_count}/{len(following_list)}", inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    print(error)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                else:
                    await asyncio.sleep(60)

class AutoLeagueAccountView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        if discord_profile:
            autoleague_settings = discord_profile.get("autoleague", {})
            current_status = autoleague_settings.get("active", False)
            autoblock = autoleague_settings.get("autoblock", False)
            target_place = autoleague_settings.get("place", 1)
            
            status_desc = "Status: Disabled" if not current_status else f"Status: Enabled {'with' if autoblock else 'without'} AutoBlock, Target: {target_place}"
            options = [
                discord.SelectOption(
                    label="Enable League Saver" if not current_status else "Disable League Saver",
                    description=status_desc,
                    value="toggle",
                    emoji=CHECK_EMOJI if not current_status else FAIL_EMOJI
                )
            ]

            select = discord.ui.Select(
                placeholder="Configure League Saver",
                options=options,
                custom_id="select_leaguesaver"
            )
            select.callback = self.leaguesaver_callback
            self.add_item(select)

    async def leaguesaver_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.command_completed = True

        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)

        discord_profile = await db.discord.find_one({"_id": self.user_id})
        current_status = discord_profile.get("autoleague", {}).get("active", False) if discord_profile else False
        new_status = not current_status

        if new_status:
            view = AutoLeaguePlaceView(self.user_id)
            await view.setup()
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: League Saver {DIAMOND_TROPHY_EMOJI}",
                description=f"",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=view)
            view.message = await interaction.original_response()
        else:
            await db.discord.update_one(
                {"_id": self.user_id},
                {"$set": {"autoleague.active": False}},
                upsert=True
            )
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: League Saver {DIAMOND_TROPHY_EMOJI}",
                description=f"{CHECK_EMOJI} League saver has been **disabled**.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
        self.stop()

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
            self.stop()

class AutoLeaguePlaceView(discord.ui.View):
    def __init__(self, user_id, batch=False):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.batch = batch
        self.message = None
        self.command_completed = False

    async def setup(self):
        place_options = [
            discord.SelectOption(label=str(i), value=str(i), emoji=DIAMOND_TROPHY_EMOJI)
            for i in range(1, 16)
        ]
        select = discord.ui.Select(
            placeholder="Select target place",
            options=place_options,
            custom_id="select_place_leaguesaver"
        )
        select.callback = self.place_callback
        self.add_item(select)

    async def place_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)
        
        place = int(interaction.data['values'][0])

        await db.discord.update_one(
            {"_id": self.user_id},
            {"$set": {
                "autoleague": {
                    "target": place,
                    "active": True,
                    "autoblock": False
                }
            }}
        )
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: LeagueSaver Enabled",
            description=f"{CHECK_EMOJI} LeagueSaver has been enabled with target place **{place}**.",
            color=discord.Color.green()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )

        class AutoblockView(discord.ui.View):
            def __init__(self, parent_view):
                super().__init__(timeout=60)
                self.parent_view = parent_view
                self.command_completed = False
                self.message = None

            @discord.ui.button(label="Yes", style=discord.ButtonStyle.secondary, custom_id="yes", emoji=CHECK_EMOJI)
            async def yes_button(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
                await btn_interaction.response.defer()
                
                wait_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
                    description=f"{CHECK_EMOJI} Processing your request...",
                    color=discord.Color.teal()
                )
                wait_embed.set_author(
                    name=btn_interaction.user.display_name,
                    icon_url=btn_interaction.user.avatar.url if btn_interaction.user.avatar else btn_interaction.user.display_avatar.url
                )
                await btn_interaction.edit_original_response(embed=wait_embed, view=None)
                
                await db.discord.update_one(
                    {"_id": self.parent_view.user_id},
                    {"$set": {"autoleague.autoblock": True}}
                )
                discord_profile = await db.discord.find_one({"_id": self.parent_view.user_id})
                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Autoblock Enabled",
                    description=f"{CHECK_EMOJI} LeagueSaver setup complete with autoblock.",
                    color=discord.Color.green()
                )
                final_embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                self.command_completed = True
                await btn_interaction.edit_original_response(embed=final_embed, view=None)

            @discord.ui.button(label="No", style=discord.ButtonStyle.secondary, custom_id="no", emoji=FAIL_EMOJI)
            async def no_button(self, btn_interaction: discord.Interaction, button: discord.ui.Button):
                await btn_interaction.response.defer()
                
                wait_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
                    description=f"{CHECK_EMOJI} Processing your request...",
                    color=discord.Color.teal()
                )
                wait_embed.set_author(
                    name=btn_interaction.user.display_name,
                    icon_url=btn_interaction.user.avatar.url if btn_interaction.user.avatar else btn_interaction.user.display_avatar.url
                )
                await btn_interaction.edit_original_response(embed=wait_embed, view=None)
                
                discord_profile = await db.discord.find_one({"_id": self.parent_view.user_id})
                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Setup Complete",
                    description=f"{CHECK_EMOJI} LeagueSaver setup complete without autoblock.",
                    color=discord.Color.green()
                )
                final_embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                self.command_completed = True
                await btn_interaction.edit_original_response(embed=final_embed, view=None)

            async def on_timeout(self):
                if not self.command_completed and self.message:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                        description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=self.message.interaction_metadata.user.display_name,
                        icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                    )
                    await self.message.edit(embed=embed, view=None)

        autoblock_view = AutoblockView(self)
        autoblock_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Enable Autoblock?",
            description="Would you like to enable autoblock for LeagueSaver?",
            color=discord.Color.green()
        )
        autoblock_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )

        self.command_completed = True
        self.stop()
        await interaction.edit_original_response(embed=autoblock_embed, view=autoblock_view)
        autoblock_view.message = await interaction.original_response()

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)

async def farm_league(session, user, headers, duo_info):
    discord_id = user["discord_id"]
    duo_id = user["_id"]

    try:
        jwt_token = user["jwt_token"]
        discord_profile = await db.discord.find_one({"_id": discord_id})
        hide = discord_profile.get("hide", False)
        if duo_info is None:
            return
        duo_username = duo_info.get('username')
        duo_avatar = await getimageurl(duo_info)
        url = (f"https://duolingo-leaderboards-prod.duolingo.com/leaderboards/7d9f5dd1-8423-491a-91f2-2532052038ce/users/{duo_id}"
               f"?client_unlocked=true&get_reactions=true&_={int(time.time() * 1000)}")
        while True:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        await leaderboard_registration(user, session, duo_info)
                        return
                    leaderboard_data = await response.json()
                if not leaderboard_data or 'active' not in leaderboard_data:
                    await leaderboard_registration(user, session, duo_info)
                    return
                active_data = leaderboard_data.get('active', None)
                if active_data is None or 'cohort' not in active_data:
                    await leaderboard_registration(user, session, duo_info)
                    return
                cohort_data = active_data.get('cohort', {})
                rankings = cohort_data.get('rankings', [])
                current_user = next((user_data for user_data in rankings if user_data['user_id'] == duo_id), None)
                if current_user is None:
                    await leaderboard_registration(user, session, duo_info)
                    return

                autoblock = discord_profile.get("autoleague", {}).get("autoblock", False)
                if autoblock:
                    blocked_users = await get_blocked(duo_info)
                    if blocked_users is not None:
                        blocked_count = 0
                        for user_data in rankings:
                            user_id_to_block = user_data.get('user_id')
                            if user_id_to_block and user_id_to_block != duo_id and user_id_to_block not in blocked_users:
                                block_url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/block/{user_id_to_block}"
                                async with session.post(block_url, headers=headers) as block_response:
                                    if block_response.status == 201:
                                        blocked_users.append(user_id_to_block)
                                        blocked_count += 1
                            await asyncio.sleep(2)

                current_score = current_user['score']
                current_rank = next((index + 1 for index, user_data in enumerate(rankings) if user_data['user_id'] == duo_id), None)
                discord_profile = await db.discord.find_one({"_id": discord_id})
                target_place = discord_profile.get("autoleague", {}).get("target", None)
                if current_rank is not None and target_place is not None and current_rank <= target_place:
                    break
                target_user = rankings[target_place - 1] if target_place and target_place - 1 < len(rankings) else None
                if target_user is None:
                    break
                target_score = target_user['score']
                xp_needed = (target_score - current_score) + 100
                if xp_needed > 0:
                    task = Task(discord_id, duo_id, "leaguesaver", None, None)
                    TASKS[task.task_id] = task
                    try:
                        await farm_xp(user, xp_needed, session, duo_info, parent_task_type="leaguesaver")
                    finally:
                        if not task.status == "cancelled":
                            task.status = "completed"
                        task.end_time = datetime.now(pytz.UTC)
                await asyncio.sleep(600)
            except Exception as e:
                await leaderboard_registration(user, session, duo_info)
                await asyncio.sleep(600)
                error_channel = bot.get_channel(ERROR_ID)
                if error_channel:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League Error {DIAMOND_TROPHY_EMOJI}",
                        description=f"{FAIL_EMOJI} Error checking league for <@{discord_id}>: {e}",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if not hide:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    else:
                        user_obj = await bot.fetch_user(discord_id)
                        if user_obj:
                            error_embed.set_author(
                                name=user_obj.display_name,
                                icon_url=user_obj.avatar.url if user_obj.avatar else user_obj.display_avatar.url
                            )
                    await error_channel.send(embed=error_embed)
    finally:
        pass

async def farm_xp(profile, xp_amount, session, duo_info, parent_task_type=None):
    discord_id = profile["discord_id"]
    duo_id = profile["_id"]
    task_type = parent_task_type if parent_task_type else "xp"
    if not parent_task_type:
        task = Task(discord_id, duo_id, task_type, xp_amount, None)
        TASKS[task.task_id] = task
    def is_task_running():
        return any(
            t.discord_id == discord_id and t.task_type == task_type and t.status == "running"
            for t in TASKS.values()
        )
    retry_attempts = 0
    try:
        while retry_attempts < 10 and is_task_running():
            try:
                jwt_token = profile["jwt_token"]
                discord_profile = await db.discord.find_one({"_id": discord_id})
                hide = discord_profile.get("hide", False)
                timezone_name = profile.get("timezone", "Asia/Saigon")
                headers = await getheaders(jwt_token, duo_id)
                duo_username = duo_info.get('username')
                duo_avatar = await getimageurl(duo_info)
                normal_count = 0
                while xp_amount > 0 and is_task_running():
                    while BOT_STOPPED:
                        print(f"[LOG] Bot stopped. Sleeping for 10 minutes...")
                        await asyncio.sleep(600)
                        await check_duolingo_access()
                        continue
                    current_time = datetime.now(pytz.timezone(timezone_name))
                    max_xp_per_request = 499
                    base_xp = 30
                    max_happy_hour = 469
                    happy_hour_bonus = min(max_happy_hour, xp_amount - base_xp) if xp_amount > base_xp else 0
                    total_xp = base_xp + happy_hour_bonus
                    dataget = {
                        "awardXp": True,
                        "completedBonusChallenge": True,
                        "fromLanguage": "en",
                        "hasXpBoost": False,
                        "illustrationFormat": "svg",
                        "isFeaturedStoryInPracticeHub": True,
                        "isLegendaryMode": True,
                        "isV2Redo": False,
                        "isV2Story": False,
                        "learningLanguage": "fr",
                        "masterVersion": True,
                        "maxScore": 0,
                        "score": 0,
                        "happyHourBonusXp": happy_hour_bonus,
                        "startTime": current_time.timestamp(),
                        "endTime": datetime.now(pytz.timezone(timezone_name)).timestamp(),
                    }
                    retry_count = 0
                    while retry_count < 10:
                        try:
                            async with session.post(
                                url=f'https://stories.duolingo.com/api2/stories/fr-en-le-passeport/complete',
                                headers=headers, json=dataget
                            ) as response:
                                if response.status == 200:
                                    response_data = await response.json()
                                    xp_amount -= response_data.get('awardedXp', 0)
                                    await asyncio.sleep(1)
                                    break
                                else:
                                    retry_count += 1
                                    if retry_count < 10:
                                        await asyncio.sleep(60)
                                    else:
                                        raise Exception(f"Failed after 10 retries. Status: {response.status}")
                        except Exception as e:
                            retry_count += 1
                            if retry_count < 10:
                                await asyncio.sleep(60)
                            else:
                                raise e
                    normal_count += 1
                success_channel = bot.get_channel(SUCCESS_ID)
                if success_channel and parent_task_type:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: LeagueSaver {DIAMOND_TROPHY_EMOJI}",
                        description=f"{CHECK_EMOJI} Successfully saved league for <@{discord_id}>",
                        color=discord.Color.purple(),
                        timestamp=datetime.now()
                    )
                    if not hide:
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                    else:
                        user = await bot.fetch_user(discord_id)
                        if user:
                            embed.set_author(
                                name=user.display_name,
                                icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                            )
                    await success_channel.send(embed=embed)
                break
            except Exception as e:
                error_channel = bot.get_channel(ERROR_ID)
                if error_channel:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League Error {DIAMOND_TROPHY_EMOJI}" if parent_task_type else f"{MAX_EMOJI} DuoXPy Max: XP Error {XP_EMOJI}",
                        description=f"{FAIL_EMOJI} Error farming XP for <@{discord_id}>.",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if not hide:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    else:
                        user = await bot.fetch_user(discord_id)
                        if user:
                            error_embed.set_author(
                                name=user.display_name,
                                icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                            )
                    await error_channel.send(embed=error_embed)
                retry_attempts += 1
                if retry_attempts < 10:
                    await asyncio.sleep(60)
                    continue
                raise e
    finally:
        if not parent_task_type:
            for t in TASKS.values():
                if t.discord_id == discord_id and t.task_type == task_type and t.status == "running":
                    if not t.status == "cancelled":
                        t.status = "completed"
                    t.end_time = datetime.now(pytz.UTC)
            
async def leaderboard_registration(profile, session, duo_info):
    duo_id = profile["_id"]
    jwt_token = profile["jwt_token"]
    headers = await getheaders(jwt_token, duo_id)
    if duo_info is None:
        return
    url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings"
    async with session.get(url, headers=headers) as response:
        if response.status != 200:
            raise ValueError(f"Failed to get privacy status. Status: {response.status}")
        data = await response.json()
        privacy_settings = data.get('privacySettings', [])
        social_setting = next((setting for setting in privacy_settings if setting['id'] == 'disable_social'), None)
        was_private = social_setting['enabled'] if social_setting else False

    try:
        if was_private:
            url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings?fields=privacySettings"
            payload = {"DISABLE_SOCIAL": False}
            async with session.patch(url, headers=headers, json=payload) as response:
                if response.status != 200:
                    raise ValueError(f"Failed to set account to public. Status: {response.status}")
            
            await asyncio.sleep(2)

        await farm_xp(profile, 200, session, duo_info)

    except Exception as e:
        raise

    finally:
        if was_private:
            try:
                url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings?fields=privacySettings"
                payload = {"DISABLE_SOCIAL": True}
                async with session.patch(url, headers=headers, json=payload) as response:
                    if response.status != 200:
                        print(f"[LOG] Failed to restore private settings for user {duo_id}. Status: {response.status}")
            except Exception as e:
                print(f"[LOG] Error restoring private settings for user {duo_id}: {str(e)}")
    
async def get_blocked(duo_info: dict):
    if duo_info:
        return duo_info.get("blockedUserIds", [])
    return None
            
async def process_massunblock(interaction: discord.Interaction, process_data: dict, profile: dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id")
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Unblock {NERD_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Unblock {NERD_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "unblock" and t.status == "running":
                task_obj = t
                break

        blocked_users = await get_blocked(duo_info)
        if not blocked_users:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: No Blocked Users",
                description=f"{CHECK_EMOJI} No users are currently blocked.",
                color=discord.Color.green()
            )
            if hide:
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
            else:
                embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            return

        total_users = len(blocked_users)
        if task_obj:
            task_obj.progress = 0
            task_obj.total = total_users
            task_obj.estimated_time_left = None

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Unblock {NERD_EMOJI}",
            description=f"{CHECK_EMOJI} Task started! Use `/task` to check progress.",
            color=discord.Color.teal()
        )
        if hide:
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
        else:
            embed.set_author(name=duo_username, icon_url=duo_avatar)
        await interaction.edit_original_response(embed=embed)

        while retry_attempts < 10:
            try:
                unblocked_count = 0
                start_time = time.time()

                for i, user_id_to_unblock in enumerate(blocked_users, start=1):
                    if task_obj and task_obj.status != "running":
                        break
                    unblock_url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/block/{user_id_to_unblock}"
                    retry_count = 0
                    while retry_count < 10:
                        async with session.delete(unblock_url, headers=headers) as response:
                            if response.status == 200:
                                unblocked_count += 1
                                retry_count = 0
                                break
                            else:
                                retry_count += 1
                                if retry_count < 10:
                                    await asyncio.sleep(60)
                                else:
                                    raise Exception(f"Failed to unblock after 10 attempts. Status: {response.status}")

                    if task_obj:
                        elapsed_time = time.time() - start_time
                        task_obj.progress = i
                        task_obj.estimated_time_left = None
                        if i > 0:
                            estimated_total_time = (elapsed_time / i) * total_users
                            estimated_remaining_time = estimated_total_time - elapsed_time
                            task_obj.estimated_time_left = estimated_remaining_time
                        task_obj.gained = unblocked_count

                total_time = time.time() - start_time
                hours, remainder = divmod(int(total_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h " if hours > 0 else ""
                time_str += f"{minutes}m " if minutes > 0 else ""
                time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                if task_obj:
                    task_obj.progress = total_users
                    task_obj.estimated_time_left = 0
                    task_obj.gained = unblocked_count
                    task_obj.end_time = datetime.now(pytz.UTC)
                    if not task_obj.status == "cancelled":
                        task_obj.status = "completed"

                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Unblock Success {NERD_EMOJI}",
                    description=f"",
                    color=discord.Color.green(),
                    timestamp=datetime.now()
                )
                final_embed.add_field(name="Total Unblocked", value=f"{unblocked_count}/{total_users}", inline=True)
                final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                if hide:
                    final_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                if success_channel:
                    await success_channel.send(embed=final_embed)
                break

            except Exception as error:
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Unblock Error {NERD_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred during unblock after 10 retry attempts",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'unblocked_count' in locals() and unblocked_count > 0:
                        error_embed.add_field(name="Total Unblocked", value=f"{unblocked_count}/{total_users}", inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    print(error)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                else:
                    await asyncio.sleep(60)

class BlockedScanView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_blockedscan"
            )
            select.callback = self.blockedscan_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def blockedscan_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            self.is_completed = True
            self.stop()
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Blocked Scan",
                description=f"{CHECK_EMOJI} Please wait while we scan for blocked users...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)
                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Blocked Scan",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                blocked_user_ids = await get_blocked(duo_info)
                if blocked_user_ids is not None and len(blocked_user_ids) > 0:
                    USERS_PER_PAGE = 10
                    user_chunks = [blocked_user_ids[i:i + USERS_PER_PAGE] for i in range(0, len(blocked_user_ids), USERS_PER_PAGE)]

                    for chunk_index, user_chunk in enumerate(user_chunks):
                        current_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Blocked Scan",
                            description=f"{CHECK_EMOJI} You have blocked **{len(blocked_user_ids)}** users.\nPage {chunk_index + 1}/{len(user_chunks)}",
                            color=discord.Color.green()
                        )
                        current_embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )

                        for blocked_user_id in user_chunk:
                            profile_link = f"https://www.duolingo.com/u/{blocked_user_id}"
                            current_embed.add_field(
                                name="",
                                value=f"- [{blocked_user_id}]({profile_link})",
                                inline=False
                            )
                        self.embeds.append(current_embed)
                    
                    self.navigation_view = NavigationView(self.embeds)
                    self.navigation_view.message = await interaction.edit_original_response(embed=self.embeds[0], view=self.navigation_view)
                else:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Blocked Scan",
                        description=f"{CHECK_EMOJI} You haven't blocked any users.",
                        color=discord.Color.green()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed)
            
        except Exception as e:
            print(f"[LOG] An error occurred in blockedscan_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()

class SuperDuolingoView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False

    async def setup(self):
        profiles = await db.list_profiles(int(self.user_id))
        if not profiles:
            return
            
        options = [
            discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI) 
            for profile in profiles
        ]
        
        if options:
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_super"
            )
            select.callback = self.super_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

    async def super_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        selected_id = int(interaction.data['values'][0])
        await self.activate_super(interaction, selected_id)

    async def activate_super(self, interaction: discord.Interaction, selected_id: int):
        try:
            is_premium = await is_premium_user(interaction.user)
            profile = await db.duolingo.find_one({"_id": selected_id})
            if not profile:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error", 
                    description=f"{FAIL_EMOJI} Selected account not found.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                self.is_completed = True
                self.stop()
                return
            
            wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Super Duolingo {SUPER_EMOJI}",
                description=f"{CHECK_EMOJI} Please wait while we activate Super Duolingo for **{profile['username']}**...",
                color=discord.Color.teal()
            )
            wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=wait_embed, view=None)

            jwt_token = profile["jwt_token"]
            
            async with await get_session(direct=True) as session:
                user_info = await getduoinfo(selected_id, jwt_token, session)
                if not user_info:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url)
                    await interaction.edit_original_response(embed=embed, view=None)
                    self.is_completed = True
                    self.stop()
                    return
                duo_avatar = await getimageurl(user_info)
                duo_username = user_info.get('name', 'Unknown')
                has_super = user_info.get("hasPlus", False)

                if has_super:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Account **{profile['username']}** already has Super Duolingo.",
                        color=discord.Color.red()
                    )
                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed, view=None)
                    self.is_completed = True
                    self.stop()
                    return

                superlinks = await db.load_superlinks()

                if not superlinks:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} No superlinks are available to provide Super Duolingo.",
                        color=discord.Color.red()
                    )
                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed, view=None)
                    self.is_completed = True
                    self.stop()
                    return

                headers = await getheaders(jwt_token, selected_id)

                for superlink in superlinks:
                    secret_code = await extract_code(superlink)
                    async with session.get(
                            f"https://www.duolingo.com/2017-06-30/family-plan/invite/{secret_code}?_={int(time.time() * 1000)}",
                            headers=headers) as response:
                        if response.status != 200:
                            await asyncio.sleep(2)
                            continue

                        invite_data = await response.json()
                        if invite_data.get("isValid", False):
                            async with session.post(
                                    f"https://www.duolingo.com/2017-06-30/users/{selected_id}/family-plan/members/invite/{secret_code}",
                                    headers=headers) as invite_response:
                                if invite_response.status == 200:
                                    embed = discord.Embed(
                                        title=f"{MAX_EMOJI} DuoXPy Max: Super Duolingo {SUPER_EMOJI}",
                                        description=f"{CHECK_EMOJI} Successfully granted Super Duolingo to account **{profile['username']}**.",
                                        color=discord.Color.green()
                                    )
                                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                                    await interaction.edit_original_response(embed=embed, view=None)
                                    self.is_completed = True
                                    self.stop()
                                    return

                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Failed to grant Super Duolingo on account **{profile['username']}**.",
                    color=discord.Color.red()
                )
                embed.set_author(name=duo_username, icon_url=duo_avatar)
                await interaction.edit_original_response(embed=embed, view=None)
                self.is_completed = True
                self.stop()

        except Exception as e:
            print(f"[LOG] An error occurred while activating Super Duolingo")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed, view=None)
            self.is_completed = True
            self.stop()

class SuperTrialView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        profiles = await db.list_profiles(int(self.user_id))
        if not profiles:
            return
            
        options = [
            discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI) 
            for profile in profiles
        ]
        
        if options:
            select = discord.ui.Select(
                placeholder="Select accounts",
                options=options,
                max_values=len(options),
                min_values=1,
                custom_id="select_account_trial"
            )
            select.callback = self.trial_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()

    async def trial_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        selected_ids = [int(value) for value in interaction.data['values']]
        
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Super Trial {SUPER_EMOJI}",
            description=f"{CHECK_EMOJI} Please wait while we process your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)

        success_count = 0
        fail_count = 0
        
        async with await get_session(direct=True) as session:
            for selected_id in selected_ids:
                profile = await db.duolingo.find_one({"_id": selected_id})
                if not profile:
                    fail_count += 1
                    await asyncio.sleep(2)
                    continue

                jwt_token = profile["jwt_token"]
                headers = await getheaders(jwt_token, selected_id)
                
                duo_info = await getduoinfo(selected_id, jwt_token, session)
                if not duo_info:
                    fail_count += 1
                    await asyncio.sleep(2)
                    continue
                has_plus = duo_info.get("hasPlus", False)
                if has_plus:
                    fail_count += 1
                    await asyncio.sleep(2)
                    continue

                json_data = {"itemName":"immersive_subscription","productId":"com.duolingo.immersive_free_trial_subscription"}
                url = f"https://www.duolingo.com/2017-06-30/users/{selected_id}/shop-items"
                try:
                    async with session.post(url, headers=headers, json=json_data) as response:
                        res_json = await response.json()
                        if response.status == 200 and "purchaseId" in res_json:
                            success_count += 1
                        else:
                            fail_count += 1
                except:
                    fail_count += 1
                
                await asyncio.sleep(2)

        if success_count > 0 and fail_count == 0:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Super Trial {SUPER_EMOJI}",
                description=f"{CHECK_EMOJI} Successfully activated Super Trial for {success_count} account(s)",
                color=discord.Color.green()
            )
        elif success_count > 0 and fail_count > 0:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Super Trial {SUPER_EMOJI}", 
                description=f"{CHECK_EMOJI} Activated Super Trial for {success_count} account(s)\n{FAIL_EMOJI} Failed for {fail_count} account(s)",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} Failed to activate Super Trial for all {fail_count} account(s)",
                color=discord.Color.red()
            )

        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)
        self.command_completed = True
        self.stop()
                
@bot.tree.command(name="help", description="List all available commands")
@is_in_guild()
@custom_cooldown(4, CMD_COOLDOWN)
async def help_v2(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    commands = sorted(bot.tree.get_commands(), key=lambda x: x.name)
    COMMANDS_PER_PAGE = 10
    command_chunks = [commands[i:i + COMMANDS_PER_PAGE] for i in range(0, len(commands), COMMANDS_PER_PAGE)]
    
    embeds = []
    for page in range(len(command_chunks)):
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max Commands",
            description=f"{CHECK_EMOJI} Total Commands: **{len(commands)}**",
            color=discord.Color.green()
        )
        
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        
        command_list = []
        for cmd in command_chunks[page]:
            description = cmd.description or "No description available"
            command_text = f"**/{cmd.name}**\n  {description}"
            
            if hasattr(cmd, 'parameters'):
                params = []
                for param in cmd.parameters:
                    if param.name != 'self' and param.name != 'interaction':
                        param_desc = param.description or "No description"
                        params.append(f"    • {param.name}: {param_desc}")
                if params:
                    command_text += "\n" + "\n".join(params)
            
            command_list.append(command_text + "\n")
        
        embed.add_field(
            name=f"Page {page + 1}/{len(command_chunks)}",
            value="\n".join(command_list),
            inline=False
        )
        
        embeds.append(embed)

    nav_view = NavigationView(embeds)
    await interaction.followup.send(embed=embeds[0], view=nav_view, ephemeral=True)
    nav_view.message = await interaction.original_response()

async def process_block_farm(interaction: discord.Interaction, process_data: dict, profile: dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id") 
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Block {DUO_MAD_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Block {DUO_MAD_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "block" and t.status == "running":
                task_obj = t
                break

        while retry_attempts < 10:
            try:
                headers = await getheaders(jwt_token, duo_id)
                leaderboard_url = f"https://duolingo-leaderboards-prod.duolingo.com/leaderboards/7d9f5dd1-8423-491a-91f2-2532052038ce/users/{duo_id}?client_unlocked=true&get_reactions=true&_={int(time.time() * 1000)}"
                
                async with session.get(leaderboard_url, headers=headers) as response:
                    if response.status != 200:
                        await leaderboard_registration(profile, session, duo_info)
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Block {DUO_MAD_EMOJI}",
                            description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                            color=discord.Color.green()
                        )
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                        await interaction.edit_original_response(embed=embed)
                        return

                    leaderboard_data = await response.json()
                    if not leaderboard_data or 'active' not in leaderboard_data:
                        await leaderboard_registration(profile, session, duo_info)
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Block {DUO_MAD_EMOJI}",
                            description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                            color=discord.Color.green()
                        )
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                        await interaction.edit_original_response(embed=embed)
                        return

                active_data = leaderboard_data.get('active')
                if not active_data or 'cohort' not in active_data:
                    await leaderboard_registration(profile, session, duo_info)
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Block {DUO_MAD_EMOJI}",
                        description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                        color=discord.Color.green()
                    )
                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed)
                    return

                rankings = active_data['cohort'].get('rankings', [])
                blocked_users = await get_blocked(duo_info)
                if blocked_users is None:
                    return

                leaderboard_users = [user['user_id'] for user in rankings if user['user_id'] != duo_id]
                users_to_block = [user_id for user_id in leaderboard_users if user_id not in blocked_users]

                total_users = len(users_to_block)
                if total_users == 0:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Block Success {DUO_MAD_EMOJI}",
                        description=f"{CHECK_EMOJI} All users are already blocked.",
                        color=discord.Color.green()
                    )
                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                    if success_channel:
                        await success_channel.send(embed=embed)
                    await interaction.edit_original_response(embed=embed)
                    return

                if task_obj:
                    task_obj.progress = 0
                    task_obj.total = total_users
                    task_obj.estimated_time_left = None

                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Block {DUO_MAD_EMOJI}",
                    description=f"{CHECK_EMOJI} Task started! Use `/task` to check progress.",
                    color=discord.Color.teal()
                )
                if hide:
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    embed.set_author(name=duo_username, icon_url=duo_avatar)
                await interaction.edit_original_response(embed=embed)

                success_count = 0
                start_time = time.time()

                for i, user_id_to_block in enumerate(users_to_block, start=1):
                    if task_obj and task_obj.status != "running":
                        break
                    block_url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/block/{user_id_to_block}"
                    async with session.post(block_url, headers=headers) as block_response:
                        if block_response.status == 201:
                            success_count += 1

                    if task_obj:
                        elapsed_time = time.time() - start_time
                        task_obj.progress = i
                        task_obj.estimated_time_left = None
                        if i > 0:
                            estimated_total_time = (elapsed_time / i) * total_users
                            estimated_remaining_time = estimated_total_time - elapsed_time
                            task_obj.estimated_time_left = estimated_remaining_time
                        task_obj.gained = success_count

                    await asyncio.sleep(1)

                total_time = time.time() - start_time
                hours, remainder = divmod(int(total_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h " if hours > 0 else ""
                time_str += f"{minutes}m " if minutes > 0 else ""
                time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                if task_obj:
                    task_obj.progress = total_users
                    task_obj.estimated_time_left = 0
                    task_obj.gained = success_count
                    task_obj.end_time = datetime.now(pytz.UTC)
                    if not task_obj.status == "cancelled":
                        task_obj.status = "completed"

                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Block Success {DUO_MAD_EMOJI}",
                    description=f"",
                    color=discord.Color.green(),
                    timestamp=datetime.now()
                )
                final_embed.add_field(name="Total Blocked", value=f"{success_count}/{total_users}", inline=True)
                final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                if hide:
                    final_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                if success_channel:
                    await success_channel.send(embed=final_embed)
                break

            except Exception as error:
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Block Error {DUO_MAD_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred during blocking after 10 retry attempts",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'success_count' in locals() and success_count > 0:
                        error_embed.add_field(name="Total Blocked", value=f"{success_count}/{total_users}", inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    print(error)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                else:
                    await asyncio.sleep(60)
    
class ListView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction):
        super().__init__(timeout=60)
        self.interaction = interaction
        self.current_page = 0
        self.message = None
        self.current_type = None
        self.is_completed = False
        
    async def setup(self):
        proxies = await db.load_proxies()
        links = await db.load_superlinks()
        keys = await db.load_keys()
        unused_keys = [key for key, data in keys.items() if isinstance(data, dict) and data.get('used_by') is None]
        
        options = [
            discord.SelectOption(label="Superlinks", value="link", description=f"{len(links)} links", emoji=SUPER_EMOJI),
            discord.SelectOption(label="Unused Keys", value="key", description=f"{len(unused_keys)} unused keys", emoji=MAX_EMOJI)
        ]
        
        select = discord.ui.Select(
            placeholder="Select content type to view",
            options=options
        )
        select.callback = self.select_callback
        self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None)
    
    async def select_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_type = interaction.data["values"][0]
        
        proxies = await db.load_proxies()
        links = await db.load_superlinks()
        keys = await db.load_keys()
        unused_keys = [key for key, data in keys.items() if isinstance(data, dict) and data.get('used_by') is None]

        if self.current_type == "proxy":
            items = list(proxies.keys())
            title = "Proxies"
        elif self.current_type == "link":
            items = list(links.keys())
            title = "Superlinks"
        else:
            items = unused_keys
            title = "Unused Premium Keys"

        chunks = [items[i:i + 10] for i in range(0, len(items), 10)]
        if not chunks:
            chunks = [[]]

        embeds = []
        for i, chunk in enumerate(chunks):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: {title}",
                description=f"{CHECK_EMOJI} Total: **{len(items)}**\nPage {i + 1}/{len(chunks)}",
                color=discord.Color.green()
            )
            embed.set_author(
                name=self.interaction.user.display_name,
                icon_url=self.interaction.user.avatar.url if self.interaction.user.avatar else self.interaction.user.display_avatar.url
            )
            if chunk:
                chunk_text = "\n".join(chunk)
                embed.add_field(
                    name="",
                    value=f"```\n{chunk_text}\n```",
                    inline=False
                )
            embeds.append(embed)

        nav_view = NavigationView(embeds)
        await interaction.edit_original_response(embed=embeds[0], view=nav_view)
        nav_view.message = await interaction.original_response()
        self.is_completed = True
        self.stop()

async def process_league_farm(interaction: discord.Interaction, process_data: dict, target_position: int, profile: dict):
    jwt_token = profile["jwt_token"]
    duo_id = profile["_id"]
    discord_id = profile.get("discord_id")
    discord_profile = await db.discord.find_one({"_id": discord_id})
    hide = discord_profile.get("hide", False)
    timezone = profile.get("timezone", "Asia/Saigon")
    headers = await getheaders(jwt_token, duo_id)
    duo_username = 'Unknown'
    duo_avatar = None
    success_channel = bot.get_channel(SUCCESS_ID)

    async with await get_session() as session:
        duo_info = await getduoinfo(duo_id, jwt_token, session)
        if not duo_info:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                description=f"{FAIL_EMOJI} Failed to retrieve Duolingo profile. Please try again later.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        duo_username = duo_info.get('name', 'Unknown')
        duo_avatar = await getimageurl(duo_info)
        is_premium = await is_premium_user(interaction.user)
        is_free = await is_premium_free_user(interaction.user)
        if (is_free or not is_premium) and not await check_name_match(duo_info):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                color=discord.Color.red()
            )
            embed.set_author(name=duo_username, icon_url=duo_avatar)
            await interaction.edit_original_response(embed=embed)
            for t in TASKS.values():
                if t.discord_id == interaction.user.id and t.status == "running":
                    t.status = "error"
                    t.end_time = datetime.now(pytz.UTC)
            return

        retry_attempts = 0
        task_obj = None
        for t in TASKS.values():
            if t.discord_id == interaction.user.id and t.task_type == "league" and t.status == "running":
                task_obj = t
                break
        if task_obj:
            task_obj.progress = 0
            task_obj.total = target_position
            task_obj.estimated_time_left = None

        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
            description=f"{CHECK_EMOJI} Task started! Use `/task` to check progress.",
            color=discord.Color.teal()
        )
        if hide:
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
        else:
            embed.set_author(name=duo_username, icon_url=duo_avatar)
        await interaction.edit_original_response(embed=embed)

        while retry_attempts < 10:
            try:
                url = f"https://duolingo-leaderboards-prod.duolingo.com/leaderboards/7d9f5dd1-8423-491a-91f2-2532052038ce/users/{duo_id}?client_unlocked=true&get_reactions=true&_={int(time.time() * 1000)}"

                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        await leaderboard_registration(profile, session, duo_info)
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                            description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                            color=discord.Color.purple()
                        )
                        if hide:
                            embed.set_author(
                                name=interaction.user.display_name,
                                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                            )
                        else:
                            embed.set_author(name=duo_username, icon_url=duo_avatar)
                        await interaction.edit_original_response(embed=embed)
                        return

                    leaderboard_data = await response.json()

                if not leaderboard_data or 'active' not in leaderboard_data:
                    await leaderboard_registration(profile, session, duo_info)
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                        description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                        color=discord.Color.purple()
                    )
                    if hide:
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed)
                    return

                active_data = leaderboard_data.get('active')
                if not active_data or 'cohort' not in active_data:
                    await leaderboard_registration(profile, session, duo_info)
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                        description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                        color=discord.Color.purple()
                    )
                    if hide:
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed)
                    return

                rankings = active_data['cohort'].get('rankings', [])
                current_user = next((user for user in rankings if user['user_id'] == duo_id), None)
                
                if not current_user:
                    await leaderboard_registration(profile, session, duo_info)
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                        description=f"{CHECK_EMOJI} Successfully joined the leaderboard! Please try again.",
                        color=discord.Color.purple()
                    )
                    if hide:
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed)
                    return

                current_position = next((i + 1 for i, user in enumerate(rankings) if user['user_id'] == duo_id), None)
                
                if current_position <= target_position:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                        description=f"{CHECK_EMOJI} You're already at **#{current_position}**!",
                        color=discord.Color.purple()
                    )
                    if hide:
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed)
                    if success_channel:
                        await success_channel.send(embed=embed)
                    return

                target_user = rankings[target_position - 1] if target_position - 1 < len(rankings) else None
                if not target_user:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                        description=f"{FAIL_EMOJI} Target position not found",
                        color=discord.Color.red()
                    )
                    if hide:
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                    await interaction.edit_original_response(embed=embed)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=embed)
                    return

                initial_score = current_user['score']
                target_score = target_user['score']
                total_xp_needed = (target_score - initial_score) + 100
                xp_remaining = total_xp_needed
                xp_count = 0
                total_xp_gained = 0
                start_time = time.time()

                while xp_remaining > 0:
                    if task_obj and task_obj.status != "running":
                        break
                    current_time = datetime.now(pytz.timezone(timezone))
                    max_xp_per_request = 499
                    base_xp = 30
                    max_happy_hour = 469
                    
                    happy_hour_bonus = min(max_happy_hour, xp_remaining - base_xp) if xp_remaining > base_xp else 0
                    total_xp = base_xp + happy_hour_bonus
                    dataget = {
                        "awardXp": True,
                        "completedBonusChallenge": True,
                        "fromLanguage": "en",
                        "hasXpBoost": False,
                        "illustrationFormat": "svg",
                        "isFeaturedStoryInPracticeHub": True,
                        "isLegendaryMode": True,
                        "isV2Redo": False,
                        "isV2Story": False,
                        "learningLanguage": "fr",
                        "masterVersion": True,
                        "maxScore": 0,
                        "score": 0,
                        "happyHourBonusXp": happy_hour_bonus,
                        "startTime": current_time.timestamp(),
                        "endTime": datetime.now(pytz.timezone(timezone)).timestamp(),
                    }

                    retry_count = 0
                    while retry_count < 10:
                        try:
                            async with session.post(
                                url=f'https://stories.duolingo.com/api2/stories/fr-en-le-passeport/complete',
                                headers=headers, json=dataget
                            ) as response:
                                if response.status == 200:
                                    response_data = await response.json()
                                    gained_xp = response_data.get('awardedXp', 0)
                                    total_xp_gained += gained_xp
                                    xp_remaining -= gained_xp
                                    if task_obj:
                                        task_obj.progress = total_xp_gained
                                        task_obj.total = total_xp_needed
                                        task_obj.gained = total_xp_gained
                                    await asyncio.sleep(1)
                                    break
                                else:
                                    retry_count += 1
                                    if retry_count < 10:
                                        await asyncio.sleep(60)
                                    else:
                                        raise Exception(f"Failed after 10 retries. Status: {response.status}")
                        except Exception as e:
                            retry_count += 1
                            if retry_count < 10:
                                await asyncio.sleep(60)
                            else:
                                raise e

                    xp_count += 1

                total_time = time.time() - start_time
                hours, remainder = divmod(int(total_time), 3600)
                minutes, seconds = divmod(remainder, 60)
                time_str = f"{hours}h " if hours > 0 else ""
                time_str += f"{minutes}m " if minutes > 0 else ""
                time_str += f"{seconds}s" if seconds > 0 or not time_str else ""

                if task_obj:
                    task_obj.progress = total_xp_needed
                    task_obj.estimated_time_left = 0
                    task_obj.gained = total_xp_gained
                    task_obj.end_time = datetime.now(pytz.UTC)
                    if not task_obj.status == "cancelled":
                        task_obj.status = "completed"

                final_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: League Success {DIAMOND_TROPHY_EMOJI}",
                    description=f"{CHECK_EMOJI} Successfully reached **#{target_position}**!",
                    color=discord.Color.purple(),
                    timestamp=datetime.now()
                )
                final_embed.add_field(name="Total XP Gained", value=total_xp_gained, inline=True)
                final_embed.add_field(name="Time Taken", value=time_str, inline=True)
                if hide:
                    final_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                else:
                    final_embed.set_author(name=duo_username, icon_url=duo_avatar)
                if success_channel:
                    await success_channel.send(embed=final_embed)
                break

            except Exception as error:
                retry_attempts += 1
                if retry_attempts >= 10:
                    error_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: League {DIAMOND_TROPHY_EMOJI}",
                        description=f"{FAIL_EMOJI} An error occurred after 10 retry attempts.",
                        color=discord.Color.red(),
                        timestamp=datetime.now()
                    )
                    if 'total_xp_gained' in locals() and total_xp_gained > 0:
                        error_embed.add_field(name="Total XP Gained", value=total_xp_gained, inline=True)
                    if 'hours' in locals() and 'minutes' in locals() and 'seconds' in locals():
                        time_str = f"{hours}h " if hours > 0 else ""
                        time_str += f"{minutes}m " if minutes > 0 else ""
                        time_str += f"{seconds}s" if seconds > 0 or not time_str else ""
                        error_embed.add_field(name="Time Elapsed", value=time_str, inline=True)
                    if hide:
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                    else:
                        error_embed.set_author(name=duo_username, icon_url=duo_avatar)
                    print(error)
                    error_channel = bot.get_channel(ERROR_ID)
                    if error_channel:
                        await error_channel.send(embed=error_embed)
                    for t in TASKS.values():
                        if t.discord_id == interaction.user.id and t.status == "running":
                            t.status = "error"
                            t.end_time = datetime.now(pytz.UTC)
                    await interaction.edit_original_response(embed=error_embed)
                else:
                    await asyncio.sleep(60)

class ResetProfileModal(discord.ui.Modal, title="Change Profile Info"):
    def __init__(self, profile, view):
        super().__init__()
        self.profile = profile
        self.view = view
        self.add_item(discord.ui.TextInput(
            label="Display Name (Optional)",
            placeholder="Enter new display name...",
            required=False
        ))
        self.add_item(discord.ui.TextInput(
            label="Username (Optional)", 
            placeholder="Enter new username...",
            required=False
        ))
        self.add_item(discord.ui.TextInput(
            label="Email (Optional)",
            placeholder="Enter new email...", 
            required=False
        ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.view.is_completed = True
        self.view.stop()
        
        jwt_token = self.profile['jwt_token']
        duo_id = self.profile['_id']
        old_username = self.profile.get('username', 'Unknown')
        
        try:
            wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
                description=f"{CHECK_EMOJI} Processing your request...",
                color=discord.Color.teal()
            )
            wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=wait_embed, view=None)

            async with await get_session(direct=True) as session:
                current_info = await getduoinfo(duo_id, jwt_token, session)
                if not current_info:
                    raise Exception("Failed to fetch current user info")

                new_name = self.children[0].value
                new_username = self.children[1].value  
                new_email = self.children[2].value

                if new_username or new_name or new_email:
                    payload = {
                        "signal": None,
                        "email": new_email or current_info.get("email", ""),
                        "name": new_name or current_info.get("name", ""),
                        "username": new_username or current_info.get("username", "")
                    }
                    
                    patch_url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}"
                    headers = await getheaders(jwt_token, duo_id)
                    async with session.patch(patch_url, headers=headers, json=payload) as patch_response:
                        if patch_response.status != 200:
                            raise Exception(f"Failed to update profile. Status: {patch_response.status}")
                        updated_info = await patch_response.json()
                        
                        if "username" in updated_info and updated_info["username"] != old_username:
                            await db.duolingo.update_one(
                                {"_id": duo_id},
                                {"$set": {"username": updated_info["username"]}}
                            )

                        changes = []
                        if "username" in updated_info and updated_info["username"] != old_username:
                            changes.append(f"Username: **{old_username} -> {updated_info['username']}**")
                        if "name" in updated_info and updated_info["name"] != current_info.get("name"):
                            changes.append(f"Name: **{current_info.get('name')} -> {updated_info['name']}**")
                        if "email" in updated_info and updated_info["email"] != current_info.get("email"):
                            changes.append(f"Email: **{current_info.get('email')} -> {updated_info['email']}**")
                        if changes:
                            description = f"{CHECK_EMOJI} Successfully updated profile:\n"
                            for change in changes:
                                description += f"{CHECK_EMOJI} {change}\n"

                            embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Profile Updated",
                                description=description,
                                color=discord.Color.green()
                            )
                        else:
                            embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: No Changes",
                                description=f"{CHECK_EMOJI} No changes were detected in the profile update.",
                                color=discord.Color.green()
                            )
                else:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: No Changes",
                        description=f"{CHECK_EMOJI} No changes were provided to update.",
                        color=discord.Color.green()
                    )

            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error", 
                description=f"{FAIL_EMOJI} Failed to update profile",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

class ResetView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = [discord.SelectOption(
                label=profile.get("username", "None") or "None",
                value=str(profile["_id"]),
                emoji=EYES_EMOJI
            ) for profile in self.profiles]
            
            if options:
                select = discord.ui.Select(
                    placeholder="Select account to modify",
                    options=options,
                    custom_id="select_account_reset"
                )
                select.callback = self.account_callback
                self.add_item(select)

    async def account_callback(self, interaction: discord.Interaction):
        selected_id = int(interaction.data['values'][0])
        selected_profile = next((p for p in self.profiles if p["_id"] == selected_id), None)
        
        if not selected_profile:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} Selected account not found.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.response.edit_message(embed=embed, view=None)
            return

        modal = ResetProfileModal(selected_profile, self)
        await interaction.response.send_modal(modal)

    async def on_timeout(self):
        if not self.is_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None)
        self.stop()

async def check_name_match(info: dict) -> bool:
    try:
        current_name = info.get("name")
        if not current_name:
            return False
        return current_name == NAME
    except Exception as e:
        print(f"[LOG] Error checking name match: {e}")
        return False

class FollowerScanView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_followerscan"
            )
            select.callback = self.followerscan_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                user = self.message.interaction_metadata.user
                embed.set_author(
                    name=user.display_name,
                    icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None)

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def followerscan_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.is_completed = True
            self.stop()
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Follower Scan",
                description=f"{CHECK_EMOJI} Please wait while we scan for followers...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)

                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Follower Scan",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/followers?viewerId={duo_id}"
                headers = await getheaders(jwt_token, duo_id)
                
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        followers = data.get('followers', {})
                        total_followers = followers.get('totalUsers', 0)
                        follower_list = followers.get('users', [])

                        if follower_list:
                            USERS_PER_PAGE = 10
                            user_chunks = [follower_list[i:i + USERS_PER_PAGE] for i in range(0, len(follower_list), USERS_PER_PAGE)]

                            for chunk_index, user_chunk in enumerate(user_chunks):
                                current_embed = discord.Embed(
                                    title=f"{MAX_EMOJI} DuoXPy Max: Follower Scan",
                                    description=f"{CHECK_EMOJI} You have **{total_followers}** followers.\nPage {chunk_index + 1}/{len(user_chunks)}",
                                    color=discord.Color.green()
                                )
                                current_embed.set_author(
                                    name=duo_username,
                                    icon_url=duo_avatar
                                )

                                for user in user_chunk:
                                    user_id = user.get('userId')
                                    username = user.get('username', 'Unknown')
                                    display_name = user.get('displayName', username)
                                    has_plus = user.get('hasSubscription', False)
                                    is_following = user.get('isFollowing', False)
                                    profile_link = f"https://www.duolingo.com/profile/{username}"
                                    user_info = (
                                        f"- [**{username}**]({profile_link})"
                                    )
                                    
                                    if has_plus:
                                        user_info += f" {SUPER_EMOJI}"
                                    if is_following:
                                        user_info += f" {EYES_EMOJI}"

                                    current_embed.add_field(
                                        name="",
                                        value=user_info,
                                        inline=False
                                    )

                                self.embeds.append(current_embed)
                            
                            self.navigation_view = NavigationView(self.embeds)
                            self.navigation_view.message = await interaction.edit_original_response(embed=self.embeds[0], view=self.navigation_view)
                        else:
                            embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Follower Scan",
                                description=f"{CHECK_EMOJI} You have no followers!",
                                color=discord.Color.green()
                            )
                            embed.set_author(
                                name=duo_username,
                                icon_url=duo_avatar
                            )
                            await interaction.edit_original_response(embed=embed)
                    else:
                        raise Exception(f"Failed to fetch followers. Status: {response.status}")
            
        except Exception as e:
            print(f"[LOG] An error occurred in followerscan_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()

class FollowingScanView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_followingscan"
            )
            select.callback = self.followingscan_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                user = self.message.interaction_metadata.user
                embed.set_author(
                    name=user.display_name,
                    icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def followingscan_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.is_completed = True
            self.stop()
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Following Scan",
                description=f"{CHECK_EMOJI} Please wait while we scan who you follow...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)

                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Following Scan",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/following?viewerId={duo_id}"
                headers = await getheaders(jwt_token, duo_id)
                
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        following = data.get('following', {})
                        total_following = following.get('totalUsers', 0)
                        following_list = following.get('users', [])

                        if following_list:
                            USERS_PER_PAGE = 10
                            user_chunks = [following_list[i:i + USERS_PER_PAGE] for i in range(0, len(following_list), USERS_PER_PAGE)]

                            for chunk_index, user_chunk in enumerate(user_chunks):
                                current_embed = discord.Embed(
                                    title=f"{MAX_EMOJI} DuoXPy Max: Following Scan",
                                    description=f"{CHECK_EMOJI} You are following **{total_following}** users.\nPage {chunk_index + 1}/{len(user_chunks)}",
                                    color=discord.Color.green()
                                )
                                current_embed.set_author(
                                    name=duo_username,
                                    icon_url=duo_avatar
                                )

                                for user in user_chunk:
                                    username = user.get('username', 'Unknown')
                                    display_name = user.get('displayName', username)
                                    has_plus = user.get('hasSubscription', False)
                                    is_followed_by = user.get('isFollowedBy', False)
                                    total_xp = user.get('totalXp', 0)
                                    profile_link = f"https://www.duolingo.com/profile/{username}"
                                    
                                    user_info = f"- [**{username}**]({profile_link})"
                                    
                                    if has_plus:
                                        user_info += f" {SUPER_EMOJI}"
                                    if is_followed_by:
                                        user_info += f" {EYES_EMOJI}"

                                    current_embed.add_field(
                                        name="",
                                        value=user_info,
                                        inline=False
                                    )

                                self.embeds.append(current_embed)
                            
                            self.navigation_view = NavigationView(self.embeds)
                            self.navigation_view.message = await interaction.edit_original_response(embed=self.embeds[0], view=self.navigation_view)
                        else:
                            embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Following Scan",
                                description=f"{CHECK_EMOJI} You are not following anyone!",
                                color=discord.Color.green()
                            )
                            embed.set_author(
                                name=duo_username,
                                icon_url=duo_avatar
                            )
                            await interaction.edit_original_response(embed=embed)
                    else:
                        raise Exception(f"Failed to fetch following list. Status: {response.status}")
            
        except Exception as e:
            print(f"[LOG] An error occurred in followingscan_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()
        
class StreakMatchView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_streakmatch"
            )
            select.callback = self.streakmatch_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def streakmatch_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.is_completed = True
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Potential Friends Match",
                description=f"{CHECK_EMOJI} Please wait while we fetch potential friends matches...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)

                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Potential Friends Match",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                headers = await getheaders(jwt_token, duo_id)
                url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/matches/potential-matches?activityName=friendsStreak&_={int(time.time() * 1000)}"
                
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        raise ValueError("Failed to fetch streak matches")
                    
                    data = await response.json()
                    matches = data.get('friendsStreak', {}).get('potentialMatches', [])

                    if matches:
                        USERS_PER_PAGE = 10
                        match_chunks = [matches[i:i + USERS_PER_PAGE] for i in range(0, len(matches), USERS_PER_PAGE)]

                        for chunk_index, match_chunk in enumerate(match_chunks):
                            current_embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Potential Friends Match",
                                description=f"{CHECK_EMOJI} Found **{len(matches)}** potential friends matches\nPage {chunk_index + 1}/{len(match_chunks)}",
                                color=discord.Color.green()
                            )
                            current_embed.set_author(
                                name=duo_username,
                                icon_url=duo_avatar
                            )

                            for match in match_chunk:
                                user_id = match.get('userId')
                                name = match.get('name', 'Unknown')
                                picture = match.get('picture', '')
                                
                                field_value = f"- [{name}](https://www.duolingo.com/u/{user_id})"
                                current_embed.add_field(
                                    name="",
                                    value=field_value,
                                    inline=False
                                )

                            self.embeds.append(current_embed)
                        
                        self.navigation_view = NavigationView(self.embeds)
                        self.navigation_view.message = await interaction.edit_original_response(embed=self.embeds[0], view=self.navigation_view)
                    else:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Potential Friends Match",
                            description=f"{CHECK_EMOJI} No potential friends matches found!",
                            color=discord.Color.green()
                        )
                        embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )
                        await interaction.edit_original_response(embed=embed)
            
        except Exception as e:
            print(f"[LOG] An error occurred in streakmatch_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()

class QuestSaverView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        if discord_profile:
            current_status = discord_profile.get("questsaver", False)
            
            options = [
                discord.SelectOption(
                    label="Enable Quest Saver" if not current_status else "Disable Quest Saver",
                    description="Status: " + ("Enabled" if current_status else "Disabled"),
                    value="toggle",
                    emoji=CHECK_EMOJI if not current_status else FAIL_EMOJI
                )
            ]

            select = discord.ui.Select(
                placeholder="Configure Quest Saver", 
                options=options,
                custom_id="select_questsaver"
            )
            select.callback = self.questsaver_callback
            self.add_item(select)
    
    async def questsaver_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.command_completed = True

        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)
        
        discord_profile = await db.discord.find_one({"_id": self.user_id})
        current_status = discord_profile.get("questsaver", False) if discord_profile else False
        new_status = not current_status
        
        await db.discord.update_one(
            {"_id": self.user_id},
            {"$set": {"questsaver": new_status}},
            upsert=True
        )
        
        status_text = "enabled" if new_status else "disabled"
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Quest Saver {QUEST_EMOJI}",
            description=f"{CHECK_EMOJI} Quest saver has been **{status_text}**.",
            color=discord.Color.green() if new_status else discord.Color.red()
        )
        
        embed.set_author(
            name=interaction.user.display_name, 
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=None)
        self.stop()

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()
     
class StreakMatchesView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_matches"
            )
            select.callback = self.matches_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                user = self.message.interaction_metadata.user
                embed.set_author(
                    name=user.display_name,
                    icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def matches_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.is_completed = True
            self.stop()
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            timezone_str = profile.get("timezone", "Asia/Saigon")
            user_timezone = pytz.timezone(timezone_str)

            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Streak Matches",
                description=f"{CHECK_EMOJI} Please wait while we fetch your streak matches...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)

                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Streak Matches",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                headers = await getheaders(jwt_token, duo_id)
                url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/matches?activityName=friendsStreak"
                
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        raise ValueError("Failed to fetch streak matches")
                    
                    data = await response.json()
                    streak_data = data.get('friendsStreak', {})
                    
                    confirmed_matches = streak_data.get('confirmedMatches', [])
                    empty_slots = streak_data.get('emptySlots', 0)
                    ended_matches = streak_data.get('endedConfirmedMatches', [])
                    pending_matches = streak_data.get('pendingMatches', [])

                    MATCHES_PER_PAGE = 5
                    all_matches = []

                    if confirmed_matches:
                        for match in confirmed_matches:
                            partner = next((user for user in match['usersInMatch'] if user['userId'] != duo_id), None)
                            if partner:
                                confirm_time = datetime.fromtimestamp(match['confirmTimestamp'])
                                local_confirm_time = user_timezone.localize(confirm_time)
                                days_active = (datetime.now() - confirm_time).days
                                
                                match_info = {
                                    'type': 'active',
                                    'partner': partner,
                                    'start_time': local_confirm_time,
                                    'days_active': days_active
                                }
                                all_matches.append(match_info)

                    if pending_matches:
                        for match in pending_matches:
                            partner = next((user for user in match['usersInMatch'] if user['userId'] != duo_id), None)
                            if partner:
                                invite_time = datetime.fromtimestamp(match['inviteTimestamp'])
                                local_invite_time = user_timezone.localize(invite_time)
                                
                                match_info = {
                                    'type': 'pending',
                                    'partner': partner,
                                    'invite_time': local_invite_time
                                }
                                all_matches.append(match_info)

                    if ended_matches:
                        for match in ended_matches:
                            partner = next((user for user in match['usersInMatch'] if user['userId'] != duo_id), None)
                            if partner:
                                end_time = datetime.fromtimestamp(match['endTimestamp'])
                                local_end_time = user_timezone.localize(end_time)
                                start_time = datetime.fromtimestamp(match['confirmTimestamp'])
                                local_start_time = user_timezone.localize(start_time)
                                duration = (end_time - start_time).days
                                
                                match_info = {
                                    'type': 'ended',
                                    'partner': partner,
                                    'start_time': local_start_time,
                                    'end_time': local_end_time,
                                    'duration': duration
                                }
                                all_matches.append(match_info)

                    if not all_matches:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Streak Matches",
                            description=f"{CHECK_EMOJI} You have no streak matches.\n{STREAK_EMOJI} Available slots: **{empty_slots}**",
                            color=discord.Color.green()
                        )
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                        await interaction.edit_original_response(embed=embed)
                        return

                    match_chunks = [all_matches[i:i + MATCHES_PER_PAGE] for i in range(0, len(all_matches), MATCHES_PER_PAGE)]

                    for chunk_index, chunk in enumerate(match_chunks):
                        current_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Streak Matches",
                            description=f"{CHECK_EMOJI} Available slots: **{empty_slots}**\nPage {chunk_index + 1}/{len(match_chunks)}",
                            color=discord.Color.green()
                        )
                        current_embed.set_author(name=duo_username, icon_url=duo_avatar)

                        for match in chunk:
                            partner = match['partner']
                            profile_link = f"https://www.duolingo.com/u/{partner['userId']}"

                            if match['type'] == 'active':
                                field_value = (
                                    f"**Started:** `{match['start_time'].strftime('%Y-%m-%d %H:%M:%S')}`\n"
                                    f"**Days Active:** `{match['days_active']}`\n"
                                    f"**Profile:** [{partner['userId']}]({profile_link})"
                                )
                                field_name = f"{STREAK_EMOJI} {partner['name']}"

                            elif match['type'] == 'pending':
                                field_value = (
                                    f"**Invited:** `{match['invite_time'].strftime('%Y-%m-%d %H:%M:%S')}`\n"
                                    f"**Profile:** [{partner['userId']}]({profile_link})"
                                )
                                field_name = f"{HOME_EMOJI} {partner['name']}"

                            else:
                                field_value = (
                                    f"**Started:** `{match['start_time'].strftime('%Y-%m-%d %H:%M:%S')}` ({timezone_str})\n"
                                    f"**Ended:** `{match['end_time'].strftime('%Y-%m-%d %H:%M:%S')}`\n"
                                    f"**Duration:** `{match['duration']}` days\n"
                                    f"**Profile:** [{partner['userId']}]({profile_link})"
                                )
                                field_name = f"{FAIL_EMOJI} {partner['name']}"

                            current_embed.add_field(
                                name=field_name,
                                value=field_value,
                                inline=False
                            )

                        self.embeds.append(current_embed)

                    self.navigation_view = NavigationView(self.embeds)
                    self.navigation_view.message = await interaction.edit_original_response(
                        embed=self.embeds[0],
                        view=self.navigation_view
                    )

        except Exception as e:
            print(f"[LOG] An error occurred in matches_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()

class FeedView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_feed"
            )
            select.callback = self.feed_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                user = self.message.interaction_metadata.user
                embed.set_author(
                    name=user.display_name,
                    icon_url=user.avatar.url if user.avatar else user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

        if self.navigation_timeout and not self.navigation_timeout.done():
            self.navigation_timeout.cancel()

    async def feed_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.is_completed = True
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            timezone_str = profile.get("timezone", "Asia/Saigon")
            user_timezone = pytz.timezone(timezone_str)

            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Feed",
                description=f"{CHECK_EMOJI} Please wait while we fetch your feed...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)

                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Feed",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                headers = await getheaders(jwt_token, duo_id)
                url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/feed/v2?uiLanguage=en"
                
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        feed_sections = data.get('feed', [])
                        feed_cards = []
                        
                        for section in feed_sections:
                            feed_cards.extend(section.get('feedCards', []))

                        if feed_cards:
                            ITEMS_PER_PAGE = 5
                            card_chunks = [feed_cards[i:i + ITEMS_PER_PAGE] for i in range(0, len(feed_cards), ITEMS_PER_PAGE)]

                            for chunk_index, user_chunk in enumerate(card_chunks):
                                current_embed = discord.Embed(
                                    title=f"{MAX_EMOJI} DuoXPy Max: Feed",
                                    description=f"{CHECK_EMOJI} You have **{len(feed_cards)}** feeds.\nPage {chunk_index + 1}/{len(card_chunks)}",
                                    color=discord.Color.green()
                                )
                                current_embed.set_author(
                                    name=duo_username,
                                    icon_url=duo_avatar
                                )

                                for card in user_chunk:
                                    display_name = card.get('displayName', 'Unknown')
                                    body = card.get('body', '')
                                    subtitle = card.get('subtitle', '')
                                    userId = card.get('userId', 'Unknown')
                                    card_type = card.get('cardType', '')
                                    
                                    try:
                                        timestamp = datetime.strptime(subtitle, "%Y-%m-%d %H:%M:%S")
                                        local_time = user_timezone.localize(timestamp)
                                        formatted_time = local_time.strftime("%Y-%m-%d %H:%M:%S")
                                    except:
                                        formatted_time = subtitle

                                    if card_type == "FOLLOW":
                                        message = f"You followed {display_name}"
                                    elif card_type == "FOLLOW_BACK":
                                        message = f"{display_name} followed you"
                                    else:
                                        message = body if body else "No message"

                                    field_value = (
                                        f"**Message:** `{message}`\n"
                                        f"**Time:** {formatted_time} ago\n"
                                        f"**Profile:** [{userId}](https://www.duolingo.com/u/{userId})"
                                    )

                                    current_embed.add_field(
                                        name=f"{GLOBE_DUOLINGO_EMOJI} {display_name}",
                                        value=field_value,
                                        inline=False
                                    )

                                self.embeds.append(current_embed)
                            self.navigation_view = NavigationView(self.embeds)
                            self.navigation_view.message = await interaction.edit_original_response(embed=self.embeds[0], view=self.navigation_view)
                        else:
                            embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Feed",
                                description=f"{CHECK_EMOJI} No feeds found!",
                                color=discord.Color.green()
                            )
                            embed.set_author(
                                name=duo_username,
                                icon_url=duo_avatar
                            )
                            await interaction.edit_original_response(embed=embed)
                    else:
                        raise Exception(f"Failed to fetch feed. Status: {response.status}")
            
        except Exception as e:
            print(f"[LOG] An error occurred in feed_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()

class PrivacyView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        profiles = await db.list_profiles(self.user_id)
        if profiles:
            options = []
            for profile in profiles:
                options.append(discord.SelectOption(
                    label=profile.get("username", "None") or "None", 
                    value=str(profile["_id"]),
                    emoji=EYES_EMOJI
                ))

            if options:
                select = discord.ui.Select(
                    placeholder="Select an account",
                    options=options,
                    custom_id="select_account_privacy"
                )
                select.callback = self.privacy_callback
                self.add_item(select)
    
    async def privacy_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.command_completed = True
        duo_id = int(interaction.data['values'][0])

        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Processing your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)
        
        try:
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            
            async with await get_session(direct=True) as session:
                headers = await getheaders(jwt_token, duo_id)
            
                url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings"
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        privacy_settings = data.get('privacySettings', [])
                        social_setting = next((setting for setting in privacy_settings if setting['id'] == 'disable_social'), None)
                        current_status = social_setting['enabled'] if social_setting else False
                    else:
                        raise ValueError(f"Failed to get current privacy status. Status: {response.status}")

                url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/privacy-settings?fields=privacySettings"
                payload = {"DISABLE_SOCIAL": not current_status}
                
                async with session.patch(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        new_status = "private" if not current_status else "public"
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Privacy Settings",
                            description=f"{CHECK_EMOJI} Successfully set **{profile['username']}** to **{new_status}** mode!",
                            color=discord.Color.green()
                        )
                    else:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Error",
                            description=f"{FAIL_EMOJI} Failed to update privacy settings. Status: {response.status}",
                            color=discord.Color.red()
                        )
            
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed)

        except Exception as e:
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
        self.stop()

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None
        self.stop()
        
class InternalView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None
        self.guilds = bot.guilds

    async def setup(self):
        try:
            wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
                description=f"{CHECK_EMOJI} Fetching server info...",
                color=discord.Color.teal()
            )
            if self.message and self.message.interaction_metadata:
                wait_embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            if self.message:
                await self.message.edit(embed=wait_embed, view=None)

            if not self.guilds:
                error_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} No servers found.",
                    color=discord.Color.red()
                )
                if self.message:
                    await self.message.edit(embed=error_embed, view=None)
                self.command_completed = True
                self.stop()
                return

            GUILDS_PER_PAGE = 10
            guild_chunks = [self.guilds[i:i + GUILDS_PER_PAGE] for i in range(0, len(self.guilds), GUILDS_PER_PAGE)]

            for page in range(len(guild_chunks)):
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Installed Servers",
                    description=f"{CHECK_EMOJI} Total Servers: **{len(self.guilds)}**\nPage {page + 1}/{len(guild_chunks)}",
                    color=discord.Color.green()
                )
                
                if self.message and self.message.interaction_metadata:
                    embed.set_author(
                        name=self.message.interaction_metadata.user.display_name,
                        icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                    )

                current_guilds = guild_chunks[page]

                for guild in current_guilds:
                    guild_name = guild.name
                    guild_id = guild.id
                    member_count = guild.member_count
                    invite_link = "No invite link available"
                    try:
                        invites = await guild.invites()
                        if invites:
                            invite_link = invites[0].url
                        else:
                            for channel in guild.text_channels:
                                if channel.permissions_for(guild.me).create_instant_invite:
                                    invite = await channel.create_invite(max_age=0, max_uses=0)
                                    invite_link = invite.url
                                    break
                    except Exception:
                        for channel in guild.text_channels:
                            try:
                                if channel.permissions_for(guild.me).create_instant_invite:
                                    invite = await channel.create_invite(max_age=0, max_uses=0)
                                    invite_link = invite.url
                                    break
                            except Exception:
                                continue
                        else:
                            invite_link = "Unable to fetch or create invite link"

                    embed.add_field(
                        name=guild_name,
                        value=f"**ID:** {guild_id}\n**Members:** {member_count}\n[Join Server]({invite_link})",
                        inline=False
                    )

                self.embeds.append(embed)

            nav_view = NavigationView(self.embeds)
            if self.message:
                await self.message.edit(embed=self.embeds[0], view=nav_view)
                nav_view.message = self.message
            self.command_completed = True
            self.stop()

        except Exception as e:
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred",
                color=discord.Color.red()
            )
            if self.message:
                await self.message.edit(embed=error_embed, view=None)
            self.command_completed = True
            self.stop()

class SettingsView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        options = [
            discord.SelectOption(
                label="Hide Accounts",
                description="Toggle hiding your accounts from the database", 
                value="hide",
                emoji=EYES_EMOJI
            ),
            discord.SelectOption(
                label="Streak Saver",
                description="Toggle Streak Saver for your accounts",
                value="streaksaver", 
                emoji=STREAK_EMOJI
            ),
            discord.SelectOption(
                label="Quest Saver",
                description="Toggle Quest Saver for your accounts",
                value="questsaver",
                emoji=QUEST_EMOJI
            ),
            discord.SelectOption(
                label="League Saver",
                description="Toggle League Saver for your accounts",
                value="leaguesaver",
                emoji=DIAMOND_TROPHY_EMOJI
            )
        ]
        
        select = discord.ui.Select(
            placeholder="Select a setting to configure",
            options=options,
            custom_id="select_setting"
        )
        select.callback = self.settings_callback
        self.add_item(select)

    async def settings_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            self.command_completed = True
            setting = interaction.data['values'][0]
            view = None
            if setting in ["questsaver", "leaguesaver", "streaksaver", "hide"]:
                if await is_premium_free_user(interaction.user):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Premium Required",
                        description=f"{FAIL_EMOJI} This command is only available to DuoXPy Premium users.\n{GEM_EMOJI} You can purchase our premium plan at {SHOP}\n{QUEST_EMOJI} Or using `/free` to get a free premium for 1 day.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return
            if setting == "streaksaver":
                view = StreakSaverView(self.user_id)
            elif setting == "leaguesaver":
                view = AutoLeagueAccountView(self.user_id)
            elif setting == "questsaver":
                view = QuestSaverView(self.user_id)
            elif setting == "hide":
                view = ToggleHideView(self.user_id)   
            await view.setup()
            
            if not view.children:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                    description=f"{FAIL_EMOJI} No accounts available to configure this setting.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return
            titles = {
                "streaksaver": f"Streak Saver {STREAK_EMOJI}",
                "leaguesaver": f"League Saver {DIAMOND_TROPHY_EMOJI}",
                "questsaver": f"Quest Saver {QUEST_EMOJI}",
                "hide": f"Hide Accounts {EYES_EMOJI}",
            }

            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: {titles.get(setting, 'Settings')}",
                description=f"",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=view)
            view.message = await interaction.original_response()
        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            print(f"[LOG] Error in settings_callback: {e}")

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None
        self.stop()

@bot.tree.command(name="settings", description="Configure bot settings.")
@check_bot_running()
@is_in_guild()
@is_premium()
@custom_cooldown(4, CMD_COOLDOWN)
async def settings(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    view = SettingsView(interaction.user.id)
    await view.setup()
    
    embed = discord.Embed(
        title=f"{MAX_EMOJI} DuoXPy Max: Settings",
        description="",
        color=discord.Color.green()
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
    )
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    view.message = await interaction.original_response()

class FetchView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        options = [
            discord.SelectOption(
                label="Leaderboard",
                description="Get info about your current leaderboard",
                value="leaderboard",
                emoji=DIAMOND_TROPHY_EMOJI
            ),
            discord.SelectOption(
                label="Feed", 
                description="Get your friends feed info",
                value="feed",
                emoji=GLOBE_DUOLINGO_EMOJI
            ),
            discord.SelectOption(
                label="Super Duolingo",
                description="Get info about your current Super Duolingo",
                value="super",
                emoji=SUPER_EMOJI
            ),
            discord.SelectOption(
                label="Potential Friends",
                description="Find potential friends for streak matches", 
                value="potential",
                emoji=NERD_EMOJI
            ),
            discord.SelectOption(
                label="Quest Friends",
                description="Get info about your current quest match",
                value="quest",
                emoji=QUEST_EMOJI
            ),
            discord.SelectOption(
                label="Streak Matches",
                description="Get info about your current streak matches",
                value="matches", 
                emoji=STREAK_EMOJI
            ),
            discord.SelectOption(
                label="Followers",
                description="See who follows you",
                value="followers",
                emoji=EYES_EMOJI
            ),
            discord.SelectOption(
                label="Following",
                description="See who you follow",
                value="following",
                emoji=HIDE_EMOJI
            ),
            discord.SelectOption(
                label="Blockers",
                description="See who blocked you",
                value="blockers",
                emoji=FAIL_EMOJI
            ),
            discord.SelectOption(
                label="Blocked Users",
                description="See who you've blocked",
                value="blocked",
                emoji=DUO_MAD_EMOJI
            )
        ]
        
        select = discord.ui.Select(
            placeholder="Select info to fetch",
            options=options,
            custom_id="select_fetch"
        )
        select.callback = self.fetch_callback
        self.add_item(select)

    async def fetch_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            self.command_completed = True
            fetch_type = interaction.data['values'][0]
            view = None
            
            if fetch_type == "feed":
                view = FeedView(self.user_id)
            elif fetch_type == "potential":
                view = StreakMatchView(self.user_id)
            elif fetch_type == "quest":
                view = QuestFriendView(self.user_id)
            elif fetch_type == "matches":
                view = StreakMatchesView(self.user_id)
            elif fetch_type == "leaderboard":
                view = LeaderboardView(self.user_id)
            elif fetch_type == "followers":
                view = FollowerScanView(self.user_id)
            elif fetch_type == "following":
                view = FollowingScanView(self.user_id)
            elif fetch_type == "blockers":
                view = BlockerScanView(self.user_id)
            elif fetch_type == "blocked":
                view = BlockedScanView(self.user_id)
            elif fetch_type == "super":
                view = SuperInviteView(self.user_id)
          
            setup_success = await view.setup()           
            if not setup_success and fetch_type == "info":
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} No selected account. Please select an account.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return
            if not view.children and fetch_type not in ["checkban", "info"]:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: No Data Available",
                    description=f"{FAIL_EMOJI} No accounts available to fetch this info.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return

            titles = {
                "feed": f"Friends Feed {GLOBE_DUOLINGO_EMOJI}",
                "potential": f"Potential Friends Match {NERD_EMOJI}",
                "quest": f"Quest Friends {QUEST_EMOJI}",
                "matches": f"Streak Matches {STREAK_EMOJI}",
                "leaderboard": f"Leaderboard {DIAMOND_TROPHY_EMOJI}",
                "followers": f"Follower Scan {EYES_EMOJI}",
                "following": f"Following List {HIDE_EMOJI}",
                "blockers": f"Blocker Scan {FAIL_EMOJI}",
                "blocked": f"Blocked Users {DUO_MAD_EMOJI}",
            }

            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: {titles.get(fetch_type, 'Fetch')}",
                description="",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=view)
            view.message = await interaction.original_response()
            
        except Exception as e:
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed, view=None)

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None
        self.stop()

@bot.tree.command(name="fetch", description="Fetch various info about your Duolingo accounts")
@check_bot_running()
@is_in_guild()
@is_premium()
@custom_cooldown(4, CMD_COOLDOWN)
async def fetch(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    view = FetchView(interaction.user.id)
    await view.setup()
    
    embed = discord.Embed(
        title=f"{MAX_EMOJI} DuoXPy Max: Fetch Info",
        description="",
        color=discord.Color.green()
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
    )
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    view.message = await interaction.original_response()

class TweakView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        options = [
            discord.SelectOption(
                label="Complete Quests", 
                description="Complete all quests",
                value="quest",
                emoji=QUEST_EMOJI
            ),
            discord.SelectOption(
                label="Set Status",
                description="Set custom status", 
                value="status",
                emoji=EYES_EMOJI
            ),
            discord.SelectOption(
                label="Get Items",
                description="Get free items",
                value="item",
                emoji=HIDE_EMOJI
            ),
            discord.SelectOption(
                label="Send Gift",
                description="Send gifts to other users",
                value="gift", 
                emoji=GLOBE_DUOLINGO_EMOJI
            ),
            discord.SelectOption(
                label="Force Quest",
                description="Force to start quest with any user",
                value="forcequest",
                emoji=CHECK_EMOJI
            ),
            discord.SelectOption(
                label="Force Streak Match",
                description="Bypass Duolingo limit to invite any user",
                value="forcestreak",
                emoji=STREAK_EMOJI
            ),
            discord.SelectOption(
                label="Remove Hearts",
                description="Remove hearts",
                value="removehearts",
                emoji=TRASH_EMOJI
            ),
            discord.SelectOption(
                label="Super Duolingo",
                description="Get Super",
                value="super",
                emoji=SUPER_EMOJI
            ),
            discord.SelectOption(
                label="Super Trial",
                description="Get 3 days trial of Super",
                value="supertrial",
                emoji=NERD_EMOJI
            )
        ]
        
        select = discord.ui.Select(
            placeholder="Select a feature to tweak",
            options=options,
            custom_id="select_tweak"
        )
        select.callback = self.tweak_callback
        self.add_item(select)

    async def tweak_callback(self, interaction: discord.Interaction):
        try:
            feature = interaction.data['values'][0]
            view = None
            self.command_completed = True
            self.stop()
            await interaction.response.defer(ephemeral=True)
            if feature == "quest":
                if not await is_premium_user(interaction.user):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Premium Required",
                        description=f"{FAIL_EMOJI} This command is only available to DuoXPy Premium users.\n{GEM_EMOJI} You can purchase our premium plan at {SHOP}\n{QUEST_EMOJI} Or using `/free` to get a free premium for 1 day.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return
                view = QuestView(self.user_id)
            elif feature == "forcequest":
                view = ForceQuest(self.user_id)
            elif feature == "forcestreak":
                view = ForceStreak(str(self.user_id))
            elif feature == "item":
                view = GiveItemView(self.user_id)
            elif feature == "gift":
                view = GiftView(self.user_id)
            elif feature == "status":
                view = SetStatusView(self.user_id)
            elif feature == "removehearts":
                view = HeartRemovalView(str(self.user_id))
            elif feature == "super":
                view = SuperDuolingoView(str(self.user_id))
            elif feature == "supertrial":
                view = SuperTrialView(str(self.user_id))

            await view.setup()
            
            if not view.children:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                    description=f"{FAIL_EMOJI} No accounts available for this feature.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return

            titles = {
                "quest": f"Complete Quest {QUEST_EMOJI}",
                "forcequest": f"Force Quest {DUO_MAD_EMOJI}",
                "item": f"Get Items {HIDE_EMOJI}",
                "gift": f"Send Gift {GLOBE_DUOLINGO_EMOJI}",
                "status": f"Set Status {EYES_EMOJI}",
                "removehearts": f"Heart Removal {TRASH_EMOJI}",
                "super": f"Super Duolingo {SUPER_EMOJI}",
                "supertrial": f"Super Trial {NERD_EMOJI}",
                "forcestreak": f"Force Streak {STREAK_EMOJI}",
                "reportv2": f"Report v2 {DUO_MAD_EMOJI}"
            }

            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: {titles.get(feature, 'Tweak')}",
                description="",
                color=discord.Color.green()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=view)
            view.message = await interaction.original_response()
        except Exception as e:
            print(f"[LOG] Error in tweak_callback: {e}")
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while processing your request. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None
        self.stop()

@bot.tree.command(name="tweak", description="Tweak various things in your Duolingo accounts")
@check_bot_running()
@is_in_guild()
@is_premium()
@custom_cooldown(4, CMD_COOLDOWN)
async def tweak(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    view = TweakView(interaction.user.id)
    await view.setup()
    
    embed = discord.Embed(
        title=f"{MAX_EMOJI} DuoXPy Max: Tweak",
        description="",
        color=discord.Color.green()
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
    )
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    view.message = await interaction.original_response()

class ListAccountsView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if not self.profiles:
            return False
            
        ACCOUNTS_PER_PAGE = 10
        account_chunks = [self.profiles[i:i + ACCOUNTS_PER_PAGE] 
                         for i in range(0, len(self.profiles), ACCOUNTS_PER_PAGE)]
        
        selected_profile = await db.get_selected_profile(self.user_id)
        user = await bot.fetch_user(self.user_id)
        
        for chunk_index, accounts in enumerate(account_chunks):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Linked Accounts",
                description=f"{CHECK_EMOJI} Total Accounts: **{len(self.profiles)}**\nPage {chunk_index + 1}/{len(account_chunks)}",
                color=discord.Color.green()
            )
            
            embed.set_author(
                name=user.display_name,
                icon_url=user.avatar.url if user.avatar else user.display_avatar.url
            )
            
            for profile in accounts:
                username = profile["username"]
                profile_link = f"https://www.duolingo.com/profile/{username}"
                
                status_tags = []

                if selected_profile and selected_profile["_id"] == profile["_id"]:
                    status_tags.append("[Selected]")
                
                status_text = " ".join(status_tags)
                
                embed.add_field(
                    name="",
                    value=f"- [{username}]({profile_link}) {status_text}",
                    inline=False
                )
            
            self.embeds.append(embed)
        
        self.navigation_view = NavigationView(self.embeds)
        return True

class CheckBanView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        profiles = await db.list_profiles(self.user_id)
        if not profiles:
            return False
        
        self.profiles = profiles
        return True

    async def check_accounts(self):
        results = []
        session = await get_session(direct=True)

        for profile in self.profiles:
            try:
                duo_id = profile["_id"]
                jwt_token = profile["jwt_token"]
                headers = await getheaders(jwt_token, duo_id)
                async with session.get(f"https://www.duolingo.com/2017-06-30/users/{duo_id}", headers=headers) as response:
                    if response.status in [400, 401]:           
                        results.append(f"{FAIL_EMOJI} **{profile['username']}**: Password may have changed. Please update credentials for this account")
                        await db.duolingo.update_one({"_id": duo_id}, {"$set": {"paused": True}})
                        await asyncio.sleep(2)
                        continue
                    elif response.status == 404:
                        await db.duolingo.delete_one({"_id": duo_id})
                        selected_profile = await db.get_selected_profile(self.user_id)
                        
                        if selected_profile and selected_profile["_id"] == duo_id:
                            remaining_profiles = await db.list_profiles(self.user_id)
                            if remaining_profiles:
                                new_profile = remaining_profiles[0]
                                await db.select_profile(self.user_id, new_profile["_id"])
                            else:
                                await db.select_profile(self.user_id, None)
                        
                        results.append(f"{FAIL_EMOJI} **{profile['username']}** account not found and has been removed")
                        await asyncio.sleep(2)
                        continue
                    elif response.status != 200:
                        print(response.status)
                        results.append(f"{FAIL_EMOJI} **{profile['username']}** cannot be checked at this time and may be banned.")
                        await asyncio.sleep(2)
                        continue
                    else:
                        duo_info = await response.json()
                        username = duo_info.get('username', profile['username'])
                        deactivated = duo_info.get('deactivated')

                        if deactivated is None:                                    
                            results.append(f"{FAIL_EMOJI} **{username}**: Password may have changed. Please update credentials for this account")
                            await asyncio.sleep(2)
                            continue
                        
                        if deactivated is True:
                            await db.duolingo.delete_one({"_id": duo_id})
                            selected_profile = await db.get_selected_profile(self.user_id)
                            
                            if selected_profile and selected_profile["_id"] == duo_id:
                                remaining_profiles = await db.list_profiles(self.user_id)
                                if remaining_profiles:
                                    new_profile = remaining_profiles[0]
                                    await db.select_profile(self.user_id, new_profile["_id"])
                                else:
                                    await db.select_profile(self.user_id, None)
                            
                            results.append(f"{FAIL_EMOJI} **{username}** is banned and has been removed")
                            await asyncio.sleep(2)
                            continue
                        results.append(f"{CHECK_EMOJI} **{username}** is not banned")

                await asyncio.sleep(1)
                
            except Exception as e:
                results.append(f"{FAIL_EMOJI} Error checking **{profile['username']}**")
                await asyncio.sleep(2)
                continue

        return results

    async def start_check(self, interaction: discord.Interaction):
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Account Status",
            description=f"{CHECK_EMOJI} Please wait while we check all your accounts status...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)

        results = await self.check_accounts()

        final_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Account Status",
            description="\n".join(results),
            color=discord.Color.green() if not any("is banned" in result for result in results) else discord.Color.red()
        )
        final_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=final_embed, view=None)

class PremiumView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        try:
            wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
                description=f"{CHECK_EMOJI} Fetching premium users...",
                color=discord.Color.teal()
            )
            if self.message and self.message.interaction_metadata:
                wait_embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            if self.message:
                await self.message.edit(embed=wait_embed, view=None)

            guild = bot.get_guild(SERVER)
            if not guild:
                error_embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Server not found.",
                    color=discord.Color.red()
                )
                if self.message:
                    await self.message.edit(embed=error_embed, view=None)
                self.command_completed = True
                self.stop()
                return

            premium_users = []
            async for discord_data in db.discord.find({"premium.active": True}):
                user_id = discord_data["_id"]
                member = guild.get_member(user_id)
                if member:
                    premium_info = discord_data["premium"]
                    end_time = premium_info.get("end")
                    premium_users.append({
                        "member": member,
                        "type": premium_info.get("type", "unknown"),
                        "duration": premium_info.get("duration"),
                        "expires_at": end_time
                    })

            for member in guild.premium_subscribers:
                if not any(p["member"].id == member.id for p in premium_users):
                    premium_users.append({
                        "member": member,
                        "type": "booster", 
                        "duration": None,
                        "expires_at": None
                    })

            if not premium_users:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Premium Users",
                    description=f"{FAIL_EMOJI} No premium users found.",
                    color=discord.Color.red()
                )
                if self.message:
                    await self.message.edit(embed=embed, view=None)
                self.command_completed = True
                self.stop()
                return

            USERS_PER_PAGE = 10
            user_chunks = [premium_users[i:i + USERS_PER_PAGE] for i in range(0, len(premium_users), USERS_PER_PAGE)]

            for page in range(len(user_chunks)):
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Premium Users",
                    description=f"{CHECK_EMOJI} Total Premium Users: **{len(premium_users)}**\nPage {page + 1}/{len(user_chunks)}",
                    color=discord.Color.green()
                )
                
                if self.message and self.message.interaction_metadata:
                    embed.set_author(
                        name=self.message.interaction_metadata.user.display_name,
                        icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                    )

                current_users = user_chunks[page]

                for user_data in current_users:
                    member = user_data["member"]
                    premium_type = user_data["type"]
                    duration = user_data["duration"]
                    expires_at = user_data["expires_at"]

                    status = []
                    if premium_type == "lifetime":
                        status.append(f"{DIAMOND_TROPHY_EMOJI} Lifetime")
                    elif premium_type == "monthly":
                        status.append(f"{GEM_EMOJI} {duration} Month{'s' if duration > 1 else ''}")
                    elif premium_type == "ultra":
                        status.append(f"{MAX_EMOJI} Ultra")
                    elif premium_type == "free":
                        status.append(f"{QUEST_EMOJI} Free")
                    elif premium_type == "booster":
                        status.append(f"{SUPER_EMOJI} Booster")

                    if expires_at:
                        expiry_date = datetime.fromtimestamp(expires_at, tz=timezone.utc)
                        status.append(f"Expires: {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}")

                    embed.add_field(
                        name="",
                        value=f"{member.mention}\n" + "\n".join(status),
                        inline=False
                    )

                self.embeds.append(embed)

            nav_view = NavigationView(self.embeds)
            if self.message:
                await self.message.edit(embed=self.embeds[0], view=nav_view)
                nav_view.message = self.message
            self.command_completed = True
            self.stop()

        except Exception as e:
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred",
                color=discord.Color.red()
            )
            if self.message:
                await self.message.edit(embed=error_embed, view=None)
            self.command_completed = True
            self.stop()

@bot.tree.command(name="internal", description="Configure internal settings for the bot")
@is_in_guild()
@is_owner()
@custom_cooldown(4, CMD_COOLDOWN)
async def internal(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    view = InternalsView(interaction.user.id)
    
    embed = discord.Embed(
        title=f"{MAX_EMOJI} DuoXPy Max: Internal",
        description=f"",
        color=discord.Color.green()
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
    )
    
    await view.setup()
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    view.message = await interaction.original_response()

class InternalsView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        options = [
            discord.SelectOption(
                label="Add Data",
                description="Add proxies or superlinks",
                value="add",
                emoji=HIDE_EMOJI
            ),
            discord.SelectOption(
                label="Generate Keys",
                description="Generate premium keys",
                value="key", 
                emoji=MAX_EMOJI
            ),
            discord.SelectOption(
                label="View Database",
                description="View all database contents", 
                value="view",
                emoji=EYES_EMOJI
            ),
            discord.SelectOption(
                label="Server List",
                description="View list of servers bot is in",
                value="servers",
                emoji=GLOBE_DUOLINGO_EMOJI
            ),
            discord.SelectOption(
                label="Premium Users",
                description="View all premium users",
                value="premiums",
                emoji=MAX_EMOJI
            )
        ]

        select = discord.ui.Select(
            placeholder="Select an internal option",
            options=options,
            custom_id="select_internal"
        )
        select.callback = self.internal_callback
        self.add_item(select)

    async def internal_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            self.command_completed = True
            self.stop()
            option = interaction.data['values'][0]

            if option == "view":
                view = ListView(interaction)
                await view.setup()
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Database Contents",
                    description="",
                    color=discord.Color.green()
                ) 
                
            elif option == "key":
                view = KeyView(interaction.user.id) 
                await view.setup()
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Key Generator",
                    description="",
                    color=discord.Color.green()
                )

            elif option == "servers":
                view = InternalView(self.user_id)
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Server List",
                    description=f"",
                    color=discord.Color.teal()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=view)
                view.message = await interaction.original_response()
                await view.setup()
                return

            elif option == "premiums":
                view = PremiumView(self.user_id)
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Premium Users",
                    description="",
                    color=discord.Color.green()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=view)
                view.message = await interaction.original_response()
                await view.setup()
                return

            elif option == "add":
                view = AddView(interaction.user.id)
                await view.setup()
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Add Data",
                    description="",
                    color=discord.Color.green()
                )

            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=view)
            if view:
                view.message = await interaction.original_response()

        except Exception as e:
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed, view=None)
            print(f"[LOG] Error in internal_callback: {e}")

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None
        self.stop()

class AddView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        options = [
            discord.SelectOption(
                label="Add Proxies",
                description="Add proxies to the database",
                value="proxies",
                emoji=HIDE_EMOJI
            ),
            discord.SelectOption(
                label="Add Superlinks",
                description="Add superlinks to the database",
                value="superlinks",
                emoji=SUPER_EMOJI
            )
        ]
        
        select = discord.ui.Select(
            placeholder="Select data type to add",
            options=options,
            custom_id="select_add"
        )
        select.callback = self.add_callback
        self.add_item(select)

    async def add_callback(self, interaction: discord.Interaction):
        modal = AddDataModal(self.user_id, interaction.data['values'][0], self.message.id)
        await interaction.response.send_modal(modal)
        self.command_completed = True
        self.stop()

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
                await self.message.edit(embed=embed, view=None)
        self.stop()

class AddDataModal(discord.ui.Modal):
    def __init__(self, user_id, data_type, message_id):
        super().__init__(title="Add Data")
        self.user_id = user_id
        self.data_type = data_type
        self.message_id = message_id
        self.data_input = discord.ui.TextInput(
            label="Enter Data",
            placeholder="Enter one item per line",
            style=discord.TextStyle.paragraph,
            required=True
        )
        self.add_item(self.data_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        data = self.data_input.value

        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Please wait while we process your request...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.followup.edit_message(message_id=self.message_id, embed=wait_embed, view=None)

        if self.data_type == "proxies":
            proxy_list = [p.replace(" ", "") for p in re.split(r'\s+', data.strip()) if p]
            current_proxies = await db.load_proxies()
            added_proxies = {}
            invalid_proxies = {}
            duplicate_proxies = {}
            
            for proxy in proxy_list:
                proxy = proxy.replace(" ", "")
                if not proxy.startswith(('socks5://', 'socks4://', 'http://', 'https://')):
                    proxy = f"socks5://{proxy}"
                    
                if proxy in current_proxies:
                    duplicate_proxies[proxy] = True
                    continue
                    
                if await check_proxy_health(proxy):
                    current_proxies[proxy] = True
                    added_proxies[proxy] = True
                else:
                    invalid_proxies[proxy] = True
            
            await db.save_proxies(current_proxies)
            
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Proxy Management",
                color=discord.Color.green() if added_proxies else discord.Color.red()
            )
            
            if added_proxies:
                embed.add_field(
                    name=f"{CHECK_EMOJI} Added Proxies",
                    value=f"Successfully added {len(added_proxies)} proxies",
                    inline=False
                )
            
            if invalid_proxies:
                embed.add_field(
                    name=f"{FAIL_EMOJI} Invalid Proxies",
                    value=f"{len(invalid_proxies)} proxies failed health check",
                    inline=False
                )

            if duplicate_proxies:
                embed.add_field(
                    name=f"{EYES_EMOJI} Duplicate Proxies",
                    value=f"{len(duplicate_proxies)} proxies already exist in database",
                    inline=False
                )

        else:
            links = data.split()
            superlinks = await db.load_superlinks()
            
            added_links = []
            for l in links:
                if l not in superlinks:
                    superlinks[l] = True
                    added_links.append(l)

            await db.save_superlinks(superlinks)

            if added_links:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Superlinks Added",
                    description=f"{CHECK_EMOJI} **{len(added_links)}** superlink(s) added successfully to the database.",
                    color=discord.Color.green()
                )
            else:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: No New Superlinks", 
                    description=f"{FAIL_EMOJI} No new superlinks were added. They may already exist in the database.",
                    color=discord.Color.red()
                )

        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        
        await interaction.followup.edit_message(message_id=self.message_id, embed=embed, view=None)

class ForceStreak(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = int(user_id)
        self.profiles = []
        self.message = None
        self.command_completed = False
        self.selected_profile = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if not self.profiles:
            return
            
        options = []
        for profile in self.profiles:
            options.append(discord.SelectOption(
                label=profile.get("username", "None") or "None",
                value=str(profile["_id"]),
                emoji=EYES_EMOJI
            ))
        
        if options:
            select = discord.ui.Select(
                placeholder="Choose your account",
                options=options,
                max_values=1,
                min_values=1,
                custom_id="select_account_force_streak"
            )
            select.callback = self.account_callback
            self.add_item(select)

    async def account_callback(self, interaction: discord.Interaction):
        self.selected_profile = next((p for p in self.profiles if str(p["_id"]) == interaction.data['values'][0]), None)
        if not self.selected_profile:
            return

        modal = TargetUserModal()
        await interaction.response.send_modal(modal)
        await modal.wait()

        if modal.username:
            async with await get_session(direct=True) as session:
                target_id = await extract_duolingo_user_to_id(modal.username, session)
                if target_id:
                    await self.process_force_streak(interaction, target_id, modal.username)
                    self.command_completed = True
                    self.stop()
                else:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Could not find user with username: **{modal.username}**",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    self.command_completed = True
                    self.stop()
                    return

    async def process_force_streak(self, interaction: discord.Interaction, target_user_id: str, username: str):
        jwt_token = self.selected_profile["jwt_token"]
        duo_id = self.selected_profile["_id"]
        duo_username = None
        duo_avatar = None

        async with await get_session(direct=True) as session:
            try:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if not duo_info:
                    raise Exception("Failed to fetch account info")
                
                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)
                headers = await getheaders(jwt_token, duo_id)
                streak_url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/matches"
                streak_payload = {
                    "activityName": "friendsStreak",
                    "intendedMatches": [{"targetUserIds": [int(target_user_id)]}]
                }

                async with session.post(streak_url, headers=headers, json=streak_payload) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to start streak match. Status: {response.status}")
                    match_data = await response.json()
                    
                    if not match_data.get('friendsStreak', {}).get('success'):
                        raise Exception("Failed to start streak match")

                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Force Streak {STREAK_EMOJI}",
                        description=f"{CHECK_EMOJI} Successfully forced streak with **{username}**",
                        color=discord.Color.green()
                    )

            except Exception as e:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Failed to force streak",
                    color=discord.Color.red()
                )

            embed.set_author(name=duo_username, icon_url=duo_avatar)
            
        self.command_completed = True
        self.stop()
        await interaction.edit_original_response(embed=embed, view=None)

    async def on_timeout(self):
        if not self.command_completed and self.message:
            timeout_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            timeout_embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=timeout_embed, view=None)
        self.stop()
        
class ForceQuest(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.message = None
        self.command_completed = False
        self.selected_profile = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = [discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI) for profile in self.profiles]
            select = discord.ui.Select(
                placeholder="Choose your account",
                options=options,
                max_values=1,
                min_values=1,
                custom_id="select_account_force_quest"
            )
            select.callback = self.account_callback
            self.add_item(select)

    async def account_callback(self, interaction: discord.Interaction):
        self.selected_profile = next((p for p in self.profiles if str(p["_id"]) == interaction.data['values'][0]), None)
        if not self.selected_profile:
            return

        modal = TargetUserModal()
        await interaction.response.send_modal(modal)
        await modal.wait()

        if modal.username:
            async with await get_session(direct=True) as session:
                target_id = await extract_duolingo_user_to_id(modal.username, session)
                if target_id:
                    await self.process_force_quest(interaction, target_id, modal.username)
                    self.command_completed = True
                    self.stop()
                else:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Could not find user with username: **{modal.username}**",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    self.command_completed = True
                    self.stop()
                    return

    async def process_force_quest(self, interaction: discord.Interaction, target_user_id: str, username: str):
        jwt_token = self.selected_profile["jwt_token"]
        duo_id = self.selected_profile["_id"]
        duo_username = None
        duo_avatar = None

        async with await get_session(direct=True) as session:
            try:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if not duo_info:
                    raise Exception("Failed to fetch account info")
                
                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)
                headers = await getheaders(jwt_token, duo_id)

                check_url = f"https://www.duolingo.com/users/{duo_id}/friends-quests/match"
                async with session.get(check_url, headers=headers) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to check current match status. Status: {response.status}")
                    
                    match_data = await response.json()
                    if match_data.get('match') is not None:
                        matched_user = match_data['match']['matchedUser']
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Force Quest {QUEST_EMOJI}",
                            description=f"{FAIL_EMOJI} Cannot force quest - already in a match with **{matched_user['name']}**",
                            color=discord.Color.red()
                        )
                        embed.set_author(name=duo_username, icon_url=duo_avatar)
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                match_url = f"https://www.duolingo.com/2017-06-30/friends/users/{duo_id}/friends-quests/match"
                match_payload = {
                    "targetUserId": target_user_id
                }

                async with session.post(match_url, headers=headers, json=match_payload) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to start quest match. Status: {response.status}")
                    match_data = await response.json()
                    
                    if not match_data.get('success', False):
                        raise Exception("Failed to start quest match")

                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Force Quest {QUEST_EMOJI}",
                    description=f"{CHECK_EMOJI} Successfully forced quest with **{username}**",
                    color=discord.Color.green()
                )

            except Exception as e:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error",
                    description=f"{FAIL_EMOJI} Failed to force quest",
                    color=discord.Color.red()
                )

            embed.set_author(name=duo_username, icon_url=duo_avatar)
            
        self.command_completed = True
        self.stop()
        await interaction.edit_original_response(embed=embed, view=None)

    async def on_timeout(self):
        if not self.command_completed and self.message:
            timeout_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            timeout_embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=timeout_embed, view=None)
        self.stop()

class TargetUserModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Enter Target Username")
        self.username = None
        self.add_item(discord.ui.TextInput(
            label="Target Username",
            placeholder="Enter the Duolingo username to force quest with",
            required=True,
            custom_id="target_username"
        ))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.username = self.children[0].value
        
class QuestFriendView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.is_completed = False
        self.current_page = 0
        self.embeds = []
        self.navigation_timeout = None
        self.navigation_view = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_questfriend"
            )
            select.callback = self.questfriend_callback
            self.add_item(select)

    async def on_timeout(self):
        if not self.is_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

    async def questfriend_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.is_completed = True
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Quest Friends",
                description=f"{CHECK_EMOJI} Please wait while we fetch quest friends...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Failed to retrieve user profile data.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name, 
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)

                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Quest Friends",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                headers = await getheaders(jwt_token, duo_id)
                url = f"https://www.duolingo.com/users/{duo_id}/friends-quests/match"
                
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        raise ValueError("Failed to fetch quest match")
                    
                    data = await response.json()
                    match = data.get('match')

                    if match:
                        matched_user = match.get('matchedUser', {})
                        user = match.get('user', {})
                        
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Current Quest Match",
                            description=f"{CHECK_EMOJI} Active quest match found!",
                            color=discord.Color.green()
                        )
                        embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )
                        
                        embed.add_field(
                            name="Matched User",
                            value=f"[{matched_user.get('name')}](https://www.duolingo.com/u/{matched_user.get('id')})",
                            inline=True
                        )
                        
                        embed.add_field(
                            name="Same Course",
                            value="Yes" if match.get('matchSameCourse') else "No",
                            inline=True
                        )
                        
                        embed.add_field(
                            name="Match Triggered By User",
                            value="Yes" if match.get('userTriggeredMatch') else "No",
                            inline=True
                        )
                        
                        await interaction.edit_original_response(embed=embed)
                    else:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Quest Friends",
                            description=f"{CHECK_EMOJI} No active quest match found!",
                            color=discord.Color.green()
                        )
                        embed.set_author(
                            name=duo_username,
                            icon_url=duo_avatar
                        )
                        await interaction.edit_original_response(embed=embed)
            
        except Exception as e:
            print(f"[LOG] An error occurred in questfriend_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred. Please try again later.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name, 
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.is_completed = True
            self.stop()
            
class AccountView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False
        self.creator = DuolingoAccountCreator()

    async def setup(self):
        options = [
            discord.SelectOption(
                label="Account Info",
                description="Get info about your selected account", 
                value="info",
                emoji=MAX_EMOJI
            ),
            discord.SelectOption(
                label="Switch Account",
                description="Switch between your linked accounts",
                value="switch",
                emoji=HIDE_EMOJI
            ),
            discord.SelectOption(
                label="Update Credentials",
                description="Update your account credentials",
                value="update_credentials",
                emoji=EYES_EMOJI
            ),
            discord.SelectOption(
                label="Change Profile Info",
                description="Change display name, username or email",
                value="reset",
                emoji=TRASH_EMOJI
            ),
            discord.SelectOption(
                label="Privacy Settings",
                description="Configure privacy settings for your accounts",
                value="private",
                emoji=GLOBE_DUOLINGO_EMOJI
            ),
            discord.SelectOption(
                label="Create Account",
                description="Create a new Duolingo account",
                value="create",
                emoji=CALENDAR_EMOJI
            ),  
            discord.SelectOption(
                label="Link Account",
                description="Link your existing Duolingo account",
                value="link",
                emoji=CHECK_EMOJI
            ),
            discord.SelectOption(
                label="Unlink Account",
                description="Unlink your Duolingo account",
                value="unlink",
                emoji=FAIL_EMOJI
            ),
            discord.SelectOption(
                label="List Accounts", 
                description="List all your linked accounts",
                value="list",
                emoji=QUEST_EMOJI
            ),
            discord.SelectOption(
                label="Check Ban Status",
                description="Check if any accounts are banned",
                value="checkban",
                emoji=DUO_MAD_EMOJI
            )
        ]
        
        select = discord.ui.Select(
            placeholder="Select account action",
            options=options,
            custom_id="select_account_action"
        )
        select.callback = self.account_callback
        self.add_item(select)

    async def account_callback(self, interaction: discord.Interaction):
        try:
            action = interaction.data['values'][0]
            if action == "create":                
                modal = CreateAccountModal()
                await interaction.response.send_modal(modal)
                await modal.wait()
                
                if modal.completed:
                    if not await check_account_limit(self.user_id, interaction):
                        return
                    wait_embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Creating Account",
                        description=f"{CHECK_EMOJI} Please wait while we create your account...",
                        color=discord.Color.teal()
                    )
                    wait_embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=wait_embed, view=None)
                    self.command_completed = True
                    self.stop()
                    try:
                        if modal.password == "":
                            modal.password = "DuoXPy@2025"
                        if len(modal.password) < 8:
                            error_embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Invalid Password",
                                description=f"{FAIL_EMOJI} Password must be at least 8 characters long.",
                                color=discord.Color.red()
                            )
                            error_embed.set_author(
                                name=interaction.user.display_name,
                                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                            )
                            await interaction.edit_original_response(embed=error_embed, view=None)
                            return
                        
                        if " " in modal.password:
                            error_embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Invalid Password",
                                description=f"{FAIL_EMOJI} Password cannot contain spaces.",
                                color=discord.Color.red()
                            )
                            error_embed.set_author(
                                name=interaction.user.display_name,
                                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                            )
                            await interaction.edit_original_response(embed=error_embed, view=None)
                            return
                        
                        has_special = any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?/" for c in modal.password)
                        has_number = any(c.isdigit() for c in modal.password)
                        has_letter = any(c.isalpha() for c in modal.password)
                        
                        if not (has_special and has_number and has_letter):
                            error_embed = discord.Embed(
                                title=f"{MAX_EMOJI} DuoXPy Max: Invalid Password",
                                description=f"{FAIL_EMOJI} Password must contain at least one letter, one number, and one special character.",
                                color=discord.Color.red()
                            )
                            error_embed.set_author(
                                name=interaction.user.display_name,
                                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                            )
                            await interaction.edit_original_response(embed=error_embed, view=None)
                            return
                        
                        account_data = await self.creator.create_account(modal.password)
                        email = account_data.get("email", "Unknown")
                        await db.login(interaction.user.id, account_data["_id"], account_data["jwt_token"], account_data["username"], account_data["timezone"])
                        success_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Account Created",
                            description=(
                                f"{CHECK_EMOJI} Account created and linked successfully!\n\n"
                                f"**Email:** `{email}`\n"
                                f"**Password:** `{modal.password}`\n\n"
                                f"{FAIL_EMOJI} **Note:** If you change the password, please update the credentials in `/account`."
                            ),
                            color=discord.Color.green()
                        )
                        success_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=success_embed)

                    except Exception as e:
                        error_embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Error",
                            description=f"{FAIL_EMOJI} Failed to create account",
                            color=discord.Color.red()
                        )
                        error_embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=error_embed)
            
            else:
                self.command_completed = True
                self.stop()
                await interaction.response.defer(ephemeral=True)
                
                if action == "link":
                    if not await check_account_limit(self.user_id, interaction):
                        return
                    view = LinkView(self.user_id)
                    await view.setup_buttons()
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Link Account",
                        description="",
                        color=discord.Color.teal()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=view)
                    view.message = await interaction.original_response()
                    
                elif action == "unlink":
                    view = LogoutView(self.user_id)
                    await view.setup()
                    
                    if view.children:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Unlink",
                            description="",
                            color=discord.Color.green()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=view)
                        view.message = await interaction.original_response()
                    else:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                            description=f"{FAIL_EMOJI} No accounts available to unlink.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        
                elif action == "switch":
                    view = SelectAccount(self.user_id)
                    await view.setup()
                    
                    if view.children:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Switch Account",
                            description="",
                            color=discord.Color.green()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=view)
                        view.message = await interaction.original_response()
                    else:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                            description=f"{FAIL_EMOJI} No accounts available to switch between.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)

                elif action == "info":
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
                        description=f"{CHECK_EMOJI} Fetching account info...",
                        color=discord.Color.teal()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    
                    view = InfoView(self.user_id)
                    setup_success = await view.setup(interaction)
                                                    
                elif action == "checkban":
                    view = CheckBanView(self.user_id)
                    setup_success = await view.setup()
                    
                    if not setup_success:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                            description=f"{FAIL_EMOJI} You have no linked accounts to check.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                    else:
                        await view.start_check(interaction)
                        
                elif action == "list":
                    view = ListAccountsView(self.user_id)
                    setup_success = await view.setup()
                    
                    if not setup_success:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                            description=f"{FAIL_EMOJI} You have no linked accounts.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                    else:
                        await interaction.edit_original_response(embed=view.embeds[0], view=view.navigation_view)
                        view.navigation_view.message = await interaction.original_response()

                elif action == "update_credentials":
                    view = UpdateCredentialsView(self.user_id)
                    await view.setup()
                    
                    if not view.children:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                            description=f"{FAIL_EMOJI} You don't have any linked accounts to update.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Update Credentials",
                        description=f"",
                        color=discord.Color.green()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    
                    await interaction.edit_original_response(embed=embed, view=view)
                    view.message = await interaction.original_response()

                elif action == "reset":
                    view = ResetView(self.user_id)
                    await view.setup()
                    
                    if not view.children:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                            description=f"{FAIL_EMOJI} You don't have any linked accounts to modify.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Change Profile Info",
                        description=f"",
                        color=discord.Color.green()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    
                    await interaction.edit_original_response(embed=embed, view=view)
                    view.message = await interaction.original_response()

                elif action == "private":
                    view = PrivacyView(self.user_id)
                    await view.setup()
                    
                    if not view.children:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: No Accounts",
                            description=f"{FAIL_EMOJI} You don't have any linked accounts to configure privacy settings.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Privacy Settings",
                        description=f"",
                        color=discord.Color.green()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    
                    await interaction.edit_original_response(embed=embed, view=view)
                    view.message = await interaction.original_response()

        except Exception as e:
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed, view=None)

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None)
        self.stop()

@bot.tree.command(name="account", description="Manage your Duolingo accounts")
@check_bot_running()
@is_in_guild()
@custom_cooldown(4, CMD_COOLDOWN)
async def account(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    view = AccountView(interaction.user.id)
    await view.setup()
    
    embed = discord.Embed(
        title=f"{MAX_EMOJI} DuoXPy Max: Account Management",
        description="",
        color=discord.Color.green()
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
    )
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    view.message = await interaction.original_response()

class GiftView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.message = None
        self.command_completed = False
        self.selected_profile = None

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = [discord.SelectOption(label=profile.get("username", "None") or "None", value=str(profile["_id"]), emoji=EYES_EMOJI) for profile in self.profiles]
            select = discord.ui.Select(
                placeholder="Choose your account",
                options=options,
                max_values=1,
                min_values=1,
                custom_id="select_account_gift"
            )
            select.callback = self.account_callback
            self.add_item(select)

    async def account_callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.selected_profile = next((p for p in self.profiles if str(p["_id"]) == interaction.data['values'][0]), None)
        if not self.selected_profile:
            return

        options = [
            discord.SelectOption(
                label="Streak Freeze",
                description="Send a streak freeze gift",
                value="streak_freeze_gift",
                emoji=STREAK_EMOJI
            ),
            discord.SelectOption(
                label="XP Boost (15 min)",
                description="Send a 15-minute XP boost gift",
                value="xp_boost_15_gift",
                emoji=XP_EMOJI
            )
        ]
        
        select = discord.ui.Select(
            placeholder="Select gift type",
            options=options,
            custom_id="select_gift_type"
        )
        select.callback = self.gift_type_callback
        
        view = discord.ui.View()
        view.add_item(select)
        
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Send Gift",
            description="",
            color=discord.Color.green()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=view)

    async def gift_type_callback(self, interaction: discord.Interaction):
        gift_type = interaction.data['values'][0]

        modal = TargetUserModal()
        await interaction.response.send_modal(modal)
        await modal.wait()

        if modal.username:
            async with await get_session(direct=True) as session:
                target_id = await extract_duolingo_user_to_id(modal.username, session)
                if target_id:
                    await self.process_gift(interaction, target_id, gift_type, modal.username)
                    self.command_completed = True
                    self.stop()
                else:
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Error",
                        description=f"{FAIL_EMOJI} Could not find user with username: **{modal.username}**",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)
                    self.command_completed = True
                    self.stop()
                    return
    async def process_gift(self, interaction: discord.Interaction, target_user_id: str, gift_type: str, target_username: str):
        jwt_token = self.selected_profile["jwt_token"]
        duo_id = self.selected_profile["_id"]
        duo_username = None
        duo_avatar = None

        async with await get_session(direct=True) as session:
            try:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if not duo_info:
                    raise Exception("Failed to fetch account info")
                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)
                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Gift",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    self.command_completed = True
                    self.stop()
                    await interaction.edit_original_response(embed=embed, view=None)
                    return
                headers = await getheaders(jwt_token, duo_id)
                url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}/gifts/{target_user_id}"
                
                async with session.post(url, headers=headers, json={"itemName": gift_type}) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to send gift. Status: {response.status}")

                    gift_name = "Streak Freeze" if gift_type == "streak_freeze_gift" else "15-minute XP Boost"
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Gift Sent",
                        description=f"{CHECK_EMOJI} Successfully sent **{gift_name}** to **{target_username}**",
                        color=discord.Color.green()
                    )

            except Exception as e:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Error", 
                    description=f"{FAIL_EMOJI} Failed to send gift",
                    color=discord.Color.red()
                )

            embed.set_author(name=duo_username, icon_url=duo_avatar)
            
        self.command_completed = True
        self.stop()
        await interaction.edit_original_response(embed=embed, view=None)
        self.stop()

    async def on_timeout(self):
        if not self.command_completed and self.message:
            timeout_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            timeout_embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=timeout_embed, view=None)
        self.stop()

class SuperInviteView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = []
            for profile in self.profiles:
                options.append(discord.SelectOption(
                    label=profile.get("username", "None") or "None",
                    value=str(profile["_id"]),
                    emoji=EYES_EMOJI
                ))
            
            select = discord.ui.Select(
                placeholder="Select an account",
                options=options,
                custom_id="select_account_super"
            )
            select.callback = self.super_callback
            self.add_item(select)

    async def super_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            self.command_completed = True
            self.stop()
            duo_id = int(interaction.data['values'][0])
            profile = await db.duolingo.find_one({"_id": duo_id})
            if not profile:
                raise ValueError("Profile not found")

            jwt_token = profile["jwt_token"]
            please_wait_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Super Duolingo",
                description=f"{CHECK_EMOJI} Please wait while we fetch Super Duolingo info...",
                color=discord.Color.teal()
            )
            please_wait_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=please_wait_embed, view=None)

            async with await get_session(direct=True) as session:
                duo_info = await getduoinfo(duo_id, jwt_token, session)
                if duo_info is None:
                    raise ValueError("Failed to retrieve user profile data")
                duo_username = duo_info.get('name', 'Unknown')
                duo_avatar = await getimageurl(duo_info)
                is_premium = await is_premium_user(interaction.user)
                is_free = await is_premium_free_user(interaction.user)
                if (is_free or not is_premium) and not await check_name_match(duo_info):
                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Super Duolingo",
                        description=f"{FAIL_EMOJI} Your Duolingo name must match the required name `{NAME}`.",
                        color=discord.Color.red()
                    )
                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    self.command_completed = True
                    self.stop()
                    await interaction.edit_original_response(embed=embed, view=None)
                    return

                headers = await getheaders(jwt_token, duo_id)
                url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}"
                
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        raise ValueError("Failed to fetch shop items")
                    
                    data = await response.json()
                    shop_items = data.get('shopItems', [])
                    premium_sub = next((item for item in shop_items if item.get('itemName') == 'premium_subscription'), None)

                    if not premium_sub:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Super Duolingo",
                            description=f"{FAIL_EMOJI} No active Super Duolingo subscription found.",
                            color=discord.Color.red()
                        )
                    else:
                        sub_info = premium_sub['subscriptionInfo']
                        currency = sub_info.get('currency', 'USD')
                        price = sub_info.get('price', 0)
                        purchase_date = datetime.fromtimestamp(premium_sub.get('purchaseDate', 0))
                        expiration_date = datetime.fromtimestamp(sub_info.get('expectedExpiration', 0))

                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Super Duolingo",
                            description=f"",
                            color=discord.Color.green()
                        )

                        embed.add_field(
                            name=f"{SUPER_EMOJI} Subscription Info",
                            value=(
                                f"**Type:** {sub_info.get('type', 'Unknown').title()}\n"
                                f"**Tier:** {sub_info.get('tier', 'Unknown').replace('_', ' ').title()}\n"
                                f"**Period:** {sub_info.get('periodLength', 0)} months\n"
                                f"**Price:** {price:,.0f} {currency}\n"
                                f"**Purchase Date:** {purchase_date.strftime('%Y-%m-%d')}\n"
                                f"**Expiration:** {expiration_date.strftime('%Y-%m-%d')}"
                            ),
                            inline=False
                        )
                        trial_status = []
                        if sub_info.get('isFreeTrialPeriod'):
                            trial_status.append("In Free Trial")
                        if sub_info.get('isIntroOfferPeriod'):
                            trial_status.append("In Intro Offer")
                        if sub_info.get('isInBillingRetryPeriod'):
                            trial_status.append("In Billing Retry")
                        
                        if trial_status:
                            embed.add_field(
                                name=f"{CHECK_EMOJI} Status",
                                value="\n".join(f"- {status}" for status in trial_status),
                                inline=False
                            )

                        embed.add_field(
                            name=f"{HIDE_EMOJI} Payment Details",
                            value=(
                                f"**Renewer:** {sub_info.get('renewer', 'Unknown')}\n"
                                f"**Auto-Renew:** {'Yes' if sub_info.get('renewing') else 'No'}\n"
                                f"**Purchase ID:** `{premium_sub.get('purchaseId', 'N/A')}`\n"
                                f"**Vendor ID:** `{sub_info.get('vendorPurchaseId', 'N/A')}`"
                            ),
                            inline=False
                        )
                        family_info = premium_sub.get('familyPlanInfo')
                        if family_info:
                            embed.add_field(
                                name=f"{EYES_EMOJI} Family Plan",
                                value=(
                                    f"**Owner ID:** `{family_info.get('ownerId')}`\n"
                                    f"**Members:** {len(family_info.get('secondaryMembers', []))+1}\n"
                                    f"**Pending Invites:** {len(family_info.get('pendingInvites', []))}\n"
                                    f"**Invite Token:** `{family_info.get('inviteToken', 'N/A')}`"
                                ),
                                inline=False
                            )

                    embed.set_author(
                        name=duo_username,
                        icon_url=duo_avatar
                    )
                    await interaction.edit_original_response(embed=embed)
            
        except Exception as e:
            print(f"[LOG] An error occurred in super_callback: {e}")
            error_embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An unexpected error occurred",
                color=discord.Color.red()
            )
            error_embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=error_embed)
            self.command_completed = True
            self.stop()

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None

class TempMail:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.tempmail.lol"

    async def create_inbox(self, domain=None, prefix=None):
        headers = {
            "Content-Type": "application/json"
        }
        data = {
            "apikey": self.api_key,
            "domain": domain,
            "prefix": prefix
        }
        
        async with await get_session(direct=True) as session:
            async with session.post(f"{self.base_url}/v2/inbox/create", headers=headers, json=data) as response:
                response.raise_for_status()
                try:
                    return await response.json()
                except ValueError:
                    if response.headers.get("Content-Type") == "text/html":
                        print(f"HTML response received: {await response.text()}")
                    else:
                        print(f"Invalid JSON response: {await response.text()}")
                    raise Exception("Invalid JSON response from TempMail API")

    async def get_emails(self, inbox):
        params = {
            "apikey": self.api_key,
            "token": inbox["token"]
        }
        
        async with await get_session(direct=True) as session:
            async with session.get(f"{self.base_url}/v2/inbox", params=params) as response:
                response.raise_for_status()
                try:
                    data = await response.json()
                    emails = data["emails"]
                    return emails
                except ValueError:
                    if response.headers.get("Content-Type") == "text/html":
                        print(f"HTML response received: {await response.text()}")
                    else:
                        print(f"Invalid JSON response: {await response.text()}")
                    raise Exception("Invalid JSON response from TempMail API")

class CreateAccountModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="Create Duolingo Account")
        self.completed = False
        
        self.password = discord.ui.TextInput(
            label="Password",
            placeholder="Enter password for the new account",
            min_length=8,
            required=True
        )
        self.add_item(self.password)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.password = self.password.value
        self.completed = True
        self.stop()

class Miscellaneous:
    def randomize_mobile_user_agent(self) -> str:
        duolingo_version = "6.26.2"
        android_version = random.randint(12, 15)
        build_codes = ['AE3A', 'TQ3A', 'TP1A', 'SP2A', 'UP1A', 'RQ3A', 'RD2A', 'SD2A']
        build_date = f"{random.randint(220101, 240806)}"
        build_suffix = f"{random.randint(1, 999):03d}"
        
        devices = [
            'sdk_gphone64_x86_64',
            'Pixel 6',
            'Pixel 6 Pro',
            'Pixel 7',
            'Pixel 7 Pro', 
            'Pixel 8',
            'SM-A536B',
            'SM-S918B',
            'SM-G998B',
            'SM-N986B',
            'OnePlus 9 Pro',
            'OnePlus 10 Pro',
            'M2102J20SG',
            'M2012K11AG'
        ]
        
        device = random.choice(devices)
        build_code = random.choice(build_codes)
        
        user_agent = f"Duodroid/{duolingo_version} Dalvik/2.1.0 (Linux; U; Android {android_version}; {device} Build/{build_code}.{build_date}.{build_suffix})"
        return user_agent

    def randomize_computer_user_agent(self) -> str:
        platforms = [
            "Windows NT 10.0; Win64; x64",
            "Windows NT 10.0; WOW64",
            "Macintosh; Intel Mac OS X 10_15_7",
            "Macintosh; Intel Mac OS X 11_2_3",
            "X11; Linux x86_64",
            "X11; Linux i686",
            "X11; Ubuntu; Linux x86_64",
        ]
        
        browsers = [
            ("Chrome", f"{random.randint(90, 140)}.0.{random.randint(1000, 4999)}.0"),
            ("Firefox", f"{random.randint(80, 115)}.0"),
            ("Safari", f"{random.randint(13, 16)}.{random.randint(0, 3)}"),
            ("Edge", f"{random.randint(90, 140)}.0.{random.randint(1000, 4999)}.0"),
        ]
        
        webkit_version = f"{random.randint(500, 600)}.{random.randint(0, 99)}"
        platform = random.choice(platforms)
        browser_name, browser_version = random.choice(browsers)
        
        if browser_name == "Safari":
            user_agent = (
                f"Mozilla/5.0 ({platform}) AppleWebKit/{webkit_version} (KHTML, like Gecko) "
                f"Version/{browser_version} Safari/{webkit_version}"
            )
        elif browser_name == "Firefox":
            user_agent = f"Mozilla/5.0 ({platform}; rv:{browser_version}) Gecko/20100101 Firefox/{browser_version}"
        else:
            user_agent = (
                f"Mozilla/5.0 ({platform}) AppleWebKit/{webkit_version} (KHTML, like Gecko) "
                f"{browser_name}/{browser_version} Safari/{webkit_version}"
            )
        
        return user_agent
    
class DuolingoAccountCreator:
    def __init__(self):
        self.misc = Miscellaneous()
        self.tmp = TempMail(os.getenv('TEMPMAIL_API_KEY'))

    def generate_random_string(self, min_length: int = 4, max_length: int = 16) -> str:
        length = random.randint(min_length, max_length)
        valid_chars = string.ascii_letters + string.digits + "-._"
        username = random.choice(string.ascii_letters)
        username += ''.join(random.choices(valid_chars, k=length-1))
        return username

    async def create_account(self, password: str) -> dict:
        headers = {
            'accept': 'application/json', 
            'connection': 'Keep-Alive',
            'content-type': 'application/json',
            'host': 'android-api-cf.duolingo.com',
            'user-agent': self.misc.randomize_mobile_user_agent(),
            'x-amzn-trace-id': 'User=0'
        }

        params = {
            'fields': 'id,creationDate,fromLanguage,courses,currentCourseId,username,health,zhTw,hasPlus,joinedClassroomIds,observedClassroomIds,roles'
        }

        json_data = {
            'currentCourseId': 'DUOLINGO_FR_EN',
            'distinctId': str(uuid.uuid4()),
            'fromLanguage': 'en',
            'timezone': 'Asia/Saigon',
            'zhTw': False
        }

        try:
            async with await get_session(direct=True) as session:
                async with session.post(
                    'https://android-api-cf.duolingo.com/2023-05-23/users',
                    params=params,
                    headers=headers,
                    json=json_data
                ) as response:
                    if response.status != 200:
                        raise Exception("Failed to create unclaimed account")
                    data = await response.json()
                    duo_id = data.get("id")
                    jwt = response.headers.get("Jwt")
                    if not duo_id or not jwt:
                        raise Exception("Failed to get duo_id or jwt")

                await asyncio.sleep(2)
                username = self.generate_random_string()
                inbox = await self.tmp.create_inbox(None, username)
                if not inbox:
                    raise Exception("Failed to create email inbox")
                email = inbox["address"]
                headers = await getheaders(jwt, duo_id)
                json_data = {
                    'requests': [{
                        'body': json.dumps({
                            'age': str(random.randint(18, 50)),
                            'distinctId': f"UserId(id={duo_id})",
                            'email': email,
                            'emailPromotion': True,
                            'name': NAME,
                            'firstName': NAME,
                            'lastName': NAME,
                            'username': username,
                            'password': password,
                            'pushPromotion': True,
                            'timezone': 'Asia/Saigon'
                        }),
                        'bodyContentType': 'application/json',
                        'method': 'PATCH',
                        'url': f'/2023-05-23/users/{duo_id}?fields=id,email,name'
                    }]
                }

                async with session.post(
                    'https://android-api-cf.duolingo.com/2017-06-30/batch',
                    params={'fields': 'responses'},
                    headers=headers,
                    json=json_data
                ) as response:
                    if response.status != 200:
                        raise Exception("Failed to claim account")
                verification_url = await self.get_verification_link(inbox, max_attempts=20)
                if not verification_url:
                    raise Exception("Failed to get verification email")
                verification_code = re.search(r"verify/([^?]+)", verification_url).group(1)
                user_id = re.search(r"userId=(\d+)", verification_url).group(1)
                api_url = f"https://www.duolingo.com/2017-06-30/users/{user_id}/email-verifications/{verification_code}"
                
                verify_headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                    'connection': 'keep-alive',
                    'host': 'www.duolingo.com',
                    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': self.misc.randomize_computer_user_agent()
                }

                payload = {"verified": True}
                
                async with session.patch(
                    api_url,
                    headers=verify_headers,
                    json=payload
                ) as response:
                    if response.status != 200:
                        raise Exception("Failed to verify account")
                    data = await response.json()
                    if not data.get("success"):
                        raise Exception("Verification unsuccessful")
                await asyncio.sleep(2)
                session_data = None
                base_url = "https://www.duolingo.com"
                lesson_headers = {
                    "Authorization": f"Bearer {jwt}",
                    "Content-Type": "application/json; charset=UTF-8",
                    "Accept": "application/json; charset=UTF-8",
                    "User-Agent": self.misc.randomize_computer_user_agent(),
                    "Origin": "https://www.duolingo.com",
                    "Referer": "https://www.duolingo.com/lesson"
                }
                url = f"{base_url}/2017-06-30/sessions"
                payload = {
                    "challengeTypes": [
                        "assist", "characterIntro", "characterMatch", "characterPuzzle",
                        "characterSelect", "characterTrace", "characterWrite",
                        "completeReverseTranslation", "definition", "dialogue",
                        "extendedMatch", "extendedListenMatch", "form", "freeResponse",
                        "gapFill", "judge", "listen", "listenComplete", "listenMatch",
                        "match", "name", "listenComprehension", "listenIsolation",
                        "listenSpeak", "listenTap", "orderTapComplete", "partialListen",
                        "partialReverseTranslate", "patternTapComplete", "radioBinary",
                        "radioImageSelect", "radioListenMatch", "radioListenRecognize",
                        "radioSelect", "readComprehension", "reverseAssist", "sameDifferent",
                        "select", "selectPronunciation", "selectTranscription", "svgPuzzle",
                        "syllableTap", "syllableListenTap", "speak", "tapCloze",
                        "tapClozeTable", "tapComplete", "tapCompleteTable", "tapDescribe",
                        "translate", "transliterate", "transliterationAssist", "typeCloze",
                        "typeClozeTable", "typeComplete", "typeCompleteTable", "writeComprehension"
                    ],
                    "fromLanguage": "en",
                    "isFinalLevel": False,
                    "isV2": True,
                    "juicy": True,
                    "learningLanguage": "fr",
                    "shakeToReportEnabled": True,
                    "smartTipsVersion": 2,
                    "isCustomIntroSkill": False,
                    "isGrammarSkill": False,
                    "levelIndex": 0,
                    "pathExperiments": [],
                    "showGrammarSkillSplash": False,
                    "skillId": "fc5f14f4f4d2451e18f3f03725a5d5b1",
                    "type": "LESSON",
                    "levelSessionIndex": 0
                }

                async with session.post(url, json=payload, headers=lesson_headers) as response:
                    if response.status == 200:
                        session_data = await response.json()
                        session_id = session_data.get("id")
                        await asyncio.sleep(2)
                        url = f"{base_url}/2017-06-30/sessions/{session_id}"
                        complete_headers = lesson_headers.copy()
                        complete_headers["Idempotency-Key"] = session_id
                        complete_headers["X-Requested-With"] = "XMLHttpRequest"
                        complete_headers["User"] = str(duo_id)
                        session_data["failed"] = False
                        current_time = datetime.now(pytz.timezone("Asia/Saigon"))
                        elapsed_time = 45 + (current_time.timestamp() % 15)
                        session_data["trackingProperties"]["sum_time_taken"] = elapsed_time
                        session_data["xpGain"] = 15
                        session_data["trackingProperties"]["xp_gained"] = 15

                        activity_uuid = session_data.get("trackingProperties", {}).get("activity_uuid")
                        if not activity_uuid:
                            activity_uuid = str(uuid.uuid4())
                            session_data["trackingProperties"]["activity_uuid"] = activity_uuid

                        await session.put(url, json=session_data, headers=complete_headers)
                        await asyncio.sleep(2)
                    else:
                        raise Exception(f"Failed to complete lesson. Status: {response.status}")

                for _ in range(10):
                    current_time = datetime.now(pytz.timezone("Asia/Saigon"))
                    url = f'https://stories.duolingo.com/api2/stories/fr-en-le-passeport/complete'
                    dataget = {
                        "awardXp": True,
                        "completedBonusChallenge": True,
                        "fromLanguage": "en",
                        "hasXpBoost": False,
                        "illustrationFormat": "svg",
                        "isFeaturedStoryInPracticeHub": True,
                        "isLegendaryMode": True,
                        "isV2Redo": False,
                        "isV2Story": False,
                        "learningLanguage": "fr",
                        "masterVersion": True,
                        "maxScore": 0,
                        "score": 0,
                        "happyHourBonusXp": random.randint(0, 465),
                        "startTime": current_time.timestamp(),
                        "endTime": datetime.now(pytz.timezone("Asia/Saigon")).timestamp(),
                    }
                    retry_count = 0
                    while True:
                        async with session.post(url=url, headers=headers, json=dataget) as response:
                            if response.status == 200:
                                await asyncio.sleep(2)
                                break
                            else:
                                retry_count += 1
                                if retry_count < 10:
                                    await asyncio.sleep(60)
                                else:
                                    raise Exception(f"Failed to farm XP after 10 attempts. Status: {response.status}")
                        
                account_data = {
                    "_id": duo_id,
                    "email": email,
                    "password": password,
                    "jwt_token": jwt,
                    "timezone": "Asia/Saigon", 
                    "username": username
                }

            return account_data
        except Exception as e:
            raise Exception(f"Failed to create account")

    async def get_verification_link(self, inbox: dict, max_attempts: int = 20) -> str:
        attempt = 0
        
        while attempt < max_attempts:
            emails = await self.tmp.get_emails(inbox)
            for email in emails:
                if email["from"].endswith("duolingo.com"):
                    if "html" in email and email["html"]:
                        matches = re.findall(r'href="(https://www\.duolingo\.com/verify/[^"]+)"', email["html"])
                        if len(matches) >= 2:
                            verification_link = matches[1]
                            verification_link = verification_link.replace("&amp;", "&")
                            base_url = re.search(r"(https://www\.duolingo\.com/verify/[^?]+)", verification_link).group(1)
                            user_id = re.search(r"userId=(\d+)", verification_link).group(1)
                            return f"{base_url}?userId={user_id}"
                    match = re.search(r"https://www\.duolingo\.com/verify/[^?\s]+\?utm_campaign=verify_email&amp;target=verify_email&amp;userId=\d+", email["body"])
                    if match:
                        verification_link = match.group(0)
                        base_url = re.search(r"(https://www\.duolingo\.com/verify/[^?\s]+)", verification_link).group(1)
                        user_id = re.search(r"userId=(\d+)", verification_link).group(1)
                        return f"{base_url}?userId={user_id}"
        
            attempt += 1
            await asyncio.sleep(2) 
        return None
    
class UpdateCredentialsView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.profiles = []
        self.message = None
        self.command_completed = False

    async def setup(self):
        self.profiles = await db.list_profiles(self.user_id)
        if self.profiles:
            options = [discord.SelectOption(
                label=profile.get("username", "None") or "None", 
                value=str(profile["_id"]), 
                emoji=EYES_EMOJI
            ) for profile in self.profiles]
            
            if options:
                select = discord.ui.Select(
                    placeholder="Select account to update",
                    options=options,
                    custom_id="select_account_update"
                )
                select.callback = self.account_callback
                self.add_item(select)

    async def account_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        selected_id = interaction.data['values'][0]
        selected_profile = next((p for p in self.profiles if str(p["_id"]) == selected_id), None)
        
        if not selected_profile:
            self.command_completed = True
            self.stop()
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} Selected account not found.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            return

        view = UpdateMethodView(selected_profile, self)
        embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Update Credentials",
            description=f"",
            color=discord.Color.green()
        )
        embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=embed, view=view)
        view.message = await interaction.original_response()

    async def on_timeout(self):
        if not self.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()

class UpdateMethodView(discord.ui.View):
    def __init__(self, profile, parent_view):
        super().__init__(timeout=60)
        self.profile = profile
        self.parent_view = parent_view
        self.message = None

    @discord.ui.button(label="Update with JWT Token", style=discord.ButtonStyle.secondary, emoji=DUO_MAD_EMOJI)
    async def jwt_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = UpdateJWTModal(self.profile)
        modal.parent_view = self
        await interaction.response.send_modal(modal)
        
    @discord.ui.button(label="Update with Password", style=discord.ButtonStyle.secondary, emoji=QUEST_EMOJI)
    async def password_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = UpdatePasswordModal(self.profile)
        modal.parent_view = self
        await interaction.response.send_modal(modal)

    async def on_timeout(self):
        if not self.parent_view.command_completed and self.message:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.message.interaction_metadata.user.display_name,
                icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
            )
            await self.message.edit(embed=embed, view=None)
        self.stop()

class UpdateJWTModal(discord.ui.Modal, title="Update Account with JWT"):
    def __init__(self, profile):
        super().__init__()
        self.profile = profile
        self.parent_view = None

    jwt_token = discord.ui.TextInput(
        label="JWT Token",
        placeholder="Enter your new JWT token",
        required=True,
        min_length=10,
        max_length=2000,
        style=discord.TextStyle.paragraph
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if self.parent_view and self.parent_view.parent_view:
            self.parent_view.parent_view.command_completed = True
            self.parent_view.parent_view.stop()
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Verifying and updating JWT token...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)

        jwt_token = self.jwt_token.value.strip().replace(" ", "").replace("'", "")
        
        if not re.match(r'^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+$', jwt_token):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Invalid Token",
                description=f"{FAIL_EMOJI} The JWT token is invalid. Please ensure it's correctly formatted and try again.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            return

        try:
            duo_id = await extract_duolingo_user_id(jwt_token)
            if not duo_id:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Invalid Token",
                    description=f"{FAIL_EMOJI} Could not extract user ID from JWT token.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return

            existing_account = await db.duolingo.find_one({"_id": duo_id})
            if existing_account and str(existing_account["_id"]) != str(self.profile["_id"]):
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: Invalid Token",
                    description=f"{FAIL_EMOJI} This JWT token belongs to a different linked account.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return

            async with await get_session(direct=True) as session:
                headers = await getheaders(jwt_token, duo_id)
                url = f"https://www.duolingo.com/2017-06-30/users/{duo_id}"
                
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Update Failed",
                            description=f"{FAIL_EMOJI} Failed to verify JWT token. Please check your token or try again later.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    if str(duo_id) != str(self.profile["_id"]):
                        profile_data = await response.json()
                        username = profile_data.get("username", "Unknown")
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Invalid Token",
                            description=(
                                f"{FAIL_EMOJI} This JWT token belongs to account **{username}** which is different from "
                                f"the selected account **{self.profile['username']}**.\n"
                                f"If you want to link this new account, please use the link command instead."
                            ),
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return
                    
                    await db.duolingo.update_one(
                        {"_id": self.profile["_id"]},
                        {"$set": {"jwt_token": jwt_token}}
                    )

                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Update Successful",
                        description=f"{CHECK_EMOJI} Successfully updated JWT token for account **{self.profile['username']}**",
                        color=discord.Color.green()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)

        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Update Failed",
                description=f"{FAIL_EMOJI} An error occurred",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)

class UpdatePasswordModal(discord.ui.Modal, title="Update Account with Password"):
    def __init__(self, profile):
        super().__init__()
        self.profile = profile
        self.parent_view = None

    password = discord.ui.TextInput(
        label="New Password",
        style=discord.TextStyle.short,
        placeholder="Enter your new password",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if self.parent_view and self.parent_view.parent_view:
            self.parent_view.parent_view.command_completed = True
            self.parent_view.parent_view.stop()
        wait_embed = discord.Embed(
            title=f"{MAX_EMOJI} DuoXPy Max: Please Wait",
            description=f"{CHECK_EMOJI} Logging in to get new JWT token...",
            color=discord.Color.teal()
        )
        wait_embed.set_author(
            name=interaction.user.display_name,
            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
        )
        await interaction.edit_original_response(embed=wait_embed, view=None)

        try:
            misc = Miscellaneous()
            distinct_id = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))
            
            async with await get_session(direct=True) as session:
                login_data = {
                    "distinctId": distinct_id,
                    "identifier": self.profile["username"],
                    "password": self.password.value
                }
                
                async with session.post(
                    "https://www.duolingo.com/2017-06-30/login?fields=",
                    json=login_data,
                    headers={
                        "Host": "www.duolingo.com",
                        "User-Agent": misc.randomize_mobile_user_agent(),
                        "Accept": "application/json",
                        "X-Amzn-Trace-Id": "User=0",
                        "Accept-Encoding": "gzip, deflate, br"
                    }
                ) as response:
                    if response.status != 200:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Update Failed",
                            description=f"{FAIL_EMOJI} Invalid password. Please try again.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    jwt_token = response.headers.get('jwt')
                    if not jwt_token:
                        embed = discord.Embed(
                            title=f"{MAX_EMOJI} DuoXPy Max: Update Failed",
                            description=f"{FAIL_EMOJI} Failed to get new JWT token. Please try again.",
                            color=discord.Color.red()
                        )
                        embed.set_author(
                            name=interaction.user.display_name,
                            icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                        )
                        await interaction.edit_original_response(embed=embed, view=None)
                        return

                    await db.duolingo.update_one(
                        {"_id": self.profile["_id"]},
                        {"$set": {"jwt_token": jwt_token}}
                    )

                    embed = discord.Embed(
                        title=f"{MAX_EMOJI} DuoXPy Max: Update Successful",
                        description=f"{CHECK_EMOJI} Successfully updated credentials for account **{self.profile['username']}**",
                        color=discord.Color.green()
                    )
                    embed.set_author(
                        name=interaction.user.display_name,
                        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                    )
                    await interaction.edit_original_response(embed=embed, view=None)

        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Update Failed",
                description=f"{FAIL_EMOJI} An error occurred",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
               
class TasksView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.message = None
        self.command_completed = False

    async def setup(self):
        options = [
            discord.SelectOption(
                label="Start Task",
                description="Start a new task", 
                value="start",
                emoji=CHECK_EMOJI
            ),
            discord.SelectOption(
                label="Stop Task", 
                description="Stop your current tasks",
                value="stop",
                emoji=FAIL_EMOJI
            ),
            discord.SelectOption(
                label="View Status",
                description="View the status of your tasks",
                value="status", 
                emoji=QUEST_EMOJI
            ),
        ]
        select = discord.ui.Select(
            placeholder="Select a task action",
            options=options,
            custom_id="select_task_action"
        )
        select.callback = self.tasks_callback
        self.add_item(select)

    async def tasks_callback(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer()
            self.command_completed = True
            action = interaction.data['values'][0]
            view = None
            if action == "start":
                view = StartView(self.user_id)
            elif action == "stop":
                view = StopView(self.user_id)
            elif action == "status":
                view = TaskInfoView(interaction)
            await view.setup()
            if not view.children and action != "status":
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: No Tasks",
                    description=f"{FAIL_EMOJI} No tasks available for this action.",
                    color=discord.Color.red()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=None)
                return
            titles = {
                "start": f"Start Task {CHECK_EMOJI}",
                "stop": f"Stop Task {FAIL_EMOJI}",
                "status": f"Task Status {QUEST_EMOJI}",
            }
            if action == "status":
                nav = NavigationView(view.embeds)
                message = await interaction.edit_original_response(embed=view.embeds[0], view=nav)
                nav.message = message
            else:
                embed = discord.Embed(
                    title=f"{MAX_EMOJI} DuoXPy Max: {titles.get(action, 'Tasks')}",
                    description="",
                    color=discord.Color.green()
                )
                embed.set_author(
                    name=interaction.user.display_name,
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
                )
                await interaction.edit_original_response(embed=embed, view=view)
                view.message = await interaction.original_response()
        except Exception as e:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Error",
                description=f"{FAIL_EMOJI} An error occurred while processing your request.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=interaction.user.display_name,
                icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
            )
            await interaction.edit_original_response(embed=embed, view=None)
            print(f"[LOG] Error in tasks_callback: {e}")

    async def on_timeout(self):
        if not self.command_completed:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Timeout",
                description=f"{FAIL_EMOJI} You took too long to respond. Please try again.",
                color=discord.Color.red()
            )
            if self.message and self.message.interaction_metadata:
                embed.set_author(
                    name=self.message.interaction_metadata.user.display_name,
                    icon_url=self.message.interaction_metadata.user.avatar.url if self.message.interaction_metadata.user.avatar else self.message.interaction_metadata.user.display_avatar.url
                )
            await self.message.edit(embed=embed, view=None) if self.message else None
        self.stop()

@bot.tree.command(name="task", description="Manage your tasks")
@is_in_guild()
@is_premium()
@custom_cooldown(4, CMD_COOLDOWN)
async def task(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    view = TasksView(str(interaction.user.id))
    await view.setup()
    embed = discord.Embed(
        title=f"{MAX_EMOJI} DuoXPy Max: Tasks",
        description="",
        color=discord.Color.green()
    )
    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.avatar.url if interaction.user.avatar else interaction.user.display_avatar.url
    )
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
    view.message = await interaction.original_response()
        
class TaskInfoView(discord.ui.View):
    def __init__(self, interaction: discord.Interaction):
        super().__init__(timeout=120)
        self.user_id = str(interaction.user.id)
        self.interaction = interaction
        self.message = None
        self.embeds = []

    async def setup(self):
        user_tasks = [task for task in TASKS.values() if str(task.discord_id) == self.user_id]
        if not user_tasks:
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: No Tasks",
                description=f"{FAIL_EMOJI} You have no tasks.",
                color=discord.Color.red()
            )
            embed.set_author(
                name=self.interaction.user.display_name,
                icon_url=self.interaction.user.avatar.url if self.interaction.user.avatar else self.interaction.user.display_avatar.url
            )
            self.embeds = [embed]
            return

        user_tasks.sort(key=lambda x: (0 if x.status == "running" else 1, -x.start_time.timestamp()))

        TASKS_PER_PAGE = 5
        task_chunks = [user_tasks[i:i + TASKS_PER_PAGE] for i in range(0, len(user_tasks), TASKS_PER_PAGE)]

        for chunk_index, task_chunk in enumerate(task_chunks):
            embed = discord.Embed(
                title=f"{MAX_EMOJI} DuoXPy Max: Your Tasks",
                description=f"Page {chunk_index + 1}/{len(task_chunks)}",
                color=discord.Color.green()
            )
            for task in task_chunk:
                status_emoji = (
                    CHECK_EMOJI if task.status == "running"
                    else FAIL_EMOJI if task.status == "cancelled"
                    else CHECK_EMOJI if task.status == "completed"
                    else QUEST_EMOJI
                )
                
                task_type_emoji = {
                    "xp": XP_EMOJI,
                    "gem": GEM_EMOJI,
                    "streak": STREAK_EMOJI,
                    "league": DIAMOND_TROPHY_EMOJI,
                    "follow": EYES_EMOJI,
                    "unfollow": TRASH_EMOJI,
                    "block": DUO_MAD_EMOJI,
                    "unblock": NERD_EMOJI
                }.get(task.task_type.lower(), "")

                progress = f"{CHECK_EMOJI} Progress: {task.progress}/{task.total}" if task.progress != "inf" else "Inf/Inf"
                gained = f"{CHECK_EMOJI} Gained: {task.gained}" if task.gained != "inf" else "Inf/Inf"
                
                start_ts = int(task.start_time.timestamp())
                start_str = f"<t:{start_ts}:F> (<t:{start_ts}:R>)"
                
                if task.end_time:
                    end_ts = int(task.end_time.timestamp())
                    end_str = f"<t:{end_ts}:F> (<t:{end_ts}:R>)"
                    seconds = (task.end_time - task.start_time).total_seconds()
                    days, remainder = divmod(int(seconds), 86400)
                    hours, remainder = divmod(remainder, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    duration_str = ""
                    if days > 0:
                        duration_str += f"{days}d "
                    if hours > 0:
                        duration_str += f"{hours}h "
                    if minutes > 0:
                        duration_str += f"{minutes}m "
                    if seconds > 0 or not duration_str:
                        duration_str += f"{seconds}s"
                    duration = f"{CHECK_EMOJI} Duration: {duration_str}"
                else:
                    end_str = "N/A"
                    duration = ""
                    if task.estimated_time_left and task.estimated_time_left != "inf" and task.total != "inf":
                        est_seconds = int(task.estimated_time_left)
                        days, remainder = divmod(est_seconds, 86400)
                        hours, remainder = divmod(remainder, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        est_str = ""
                        if days > 0:
                            est_str += f"{days}d "
                        if hours > 0:
                            est_str += f"{hours}h "
                        if minutes > 0:
                            est_str += f"{minutes}m "
                        if seconds > 0 or not est_str:
                            est_str += f"{seconds}s"
                        est_ts = int((datetime.now(pytz.UTC) + timedelta(seconds=est_seconds)).timestamp())
                        duration = f"{CALENDAR_EMOJI} Estimated completion: <t:{est_ts}:R> ({est_str})"

                value = (
                    f"{CHECK_EMOJI} Task ID: `{task.task_id[:8]}`\n"
                    f"{CHECK_EMOJI} Status: {status_emoji} {task.status}\n"
                    f"{CHECK_EMOJI} Amount: {task.amount if task.amount and task.amount != 'inf' else 'N/A'}\n"
                    f"{CHECK_EMOJI} Duolingo ID: `{task.duolingo_id}`\n"
                    f"{CALENDAR_EMOJI} Start: {start_str}\n"
                    f"{CALENDAR_EMOJI} End: {end_str}\n"
                    f"{progress}\n" if progress else ""
                    f"{gained}\n" if gained else ""
                    f"{duration}\n" if duration else ""
                )
                embed.add_field(
                    name=f"{task_type_emoji} {task.task_type} Task",
                    value=value,
                    inline=False
                )
            embed.set_author(
                name=self.interaction.user.display_name,
                icon_url=self.interaction.user.avatar.url if self.interaction.user.avatar else self.interaction.user.display_avatar.url
            )
            self.embeds.append(embed)

    async def on_timeout(self):
        if self.message:
            await self.message.edit(view=None)
        self.stop()

bot.run(TOKEN)
