from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/status"), KeyboardButton(text="/stats")],
            [KeyboardButton(text="/anomalies"), KeyboardButton(text="/hot")],
        ],
        resize_keyboard=True,
    )


def top_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="OI", callback_data="top_oi"),
                InlineKeyboardButton(text="Funding", callback_data="top_funding"),
                InlineKeyboardButton(text="L/S", callback_data="top_ls"),
            ]
        ]
    )
