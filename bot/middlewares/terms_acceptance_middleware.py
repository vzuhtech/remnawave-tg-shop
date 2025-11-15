import logging
from typing import Callable, Dict, Any, Awaitable, Optional, Union

from aiogram import BaseMiddleware, Bot
from aiogram.types import Message, CallbackQuery, User, Update, InlineKeyboardMarkup
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramBadRequest, AiogramError

from config.settings import Settings
from db.dal import user_dal

from .i18n import JsonI18n
from ..keyboards.inline.user_keyboards import get_terms_acceptance_keyboard


class TermsAcceptanceMiddleware(BaseMiddleware):

    def __init__(self, settings: Settings, i18n_instance: JsonI18n):
        super().__init__()
        self.settings = settings
        self.i18n_main_instance = i18n_instance

    async def __call__(self, handler: Callable[[Update, Dict[str, Any]],
                                               Awaitable[Any]], event: Update,
                       data: Dict[str, Any]) -> Any:
        session: AsyncSession = data["session"]
        event_user: Optional[User] = data.get("event_from_user")
        bot_instance: Bot = data["bot"]

        if not event_user:
            return await handler(event, data)

        # Allow admins to bypass terms acceptance
        if event_user.id in self.settings.ADMIN_IDS:
            return await handler(event, data)

        # Get the actual event object (Message or CallbackQuery)
        actual_event_object: Optional[Union[Message, CallbackQuery]] = None
        if event.message:
            actual_event_object = event.message
        elif event.callback_query:
            actual_event_object = event.callback_query

        # Allow /start command to pass through (it will handle terms acceptance)
        if isinstance(actual_event_object, Message) and actual_event_object.text and actual_event_object.text.startswith("/start"):
            return await handler(event, data)

        # Allow callback queries related to terms acceptance
        if isinstance(actual_event_object, CallbackQuery) and actual_event_object.data:
            if actual_event_object.data.startswith("terms:"):
                return await handler(event, data)

        try:
            db_user_model = await user_dal.get_user_by_id(
                session, event_user.id)
        except Exception as e_db:
            logging.error(
                f"TermsAcceptanceMiddleware: DB error fetching user {event_user.id}: {e_db}",
                exc_info=True)
            return await handler(event, data)

        # If user doesn't exist yet, allow /start to create them
        if not db_user_model:
            return await handler(event, data)

        # Check if user has accepted terms
        if not db_user_model.terms_accepted:
            logging.info(
                f"User {event_user.id} ({event_user.username or 'NoUsername'}) has not accepted terms. Blocking access."
            )

            i18n_data_from_event = data.get("i18n_data", {})
            current_lang = i18n_data_from_event.get(
                "current_language", self.settings.DEFAULT_LANGUAGE)
            i18n_to_use: Optional[JsonI18n] = i18n_data_from_event.get(
                "i18n_instance", self.i18n_main_instance)

            terms_message_text = "Для продолжения работы необходимо ознакомиться и принять соглашения."
            keyboard: Optional[InlineKeyboardMarkup] = None

            if i18n_to_use:
                _ = lambda k, **kw: i18n_to_use.gettext(current_lang, k, **kw)
                terms_message_text = _("terms_acceptance_required")
                keyboard = get_terms_acceptance_keyboard(
                    current_lang, i18n_to_use, self.settings.TERMS_DOCUMENTS_URL)

            try:
                if isinstance(actual_event_object, Message):
                    await actual_event_object.answer(terms_message_text,
                                                     reply_markup=keyboard)
                elif isinstance(actual_event_object, CallbackQuery):
                    await actual_event_object.answer(terms_message_text,
                                                     show_alert=True)
                    if actual_event_object.message:
                        try:
                            await actual_event_object.message.edit_text(
                                terms_message_text, reply_markup=keyboard)
                        except (TelegramAPIError, AiogramError):
                            await bot_instance.send_message(
                                actual_event_object.from_user.id,
                                terms_message_text,
                                reply_markup=keyboard)
                    else:
                        await bot_instance.send_message(
                            actual_event_object.from_user.id,
                            terms_message_text,
                            reply_markup=keyboard)
                else:
                    await bot_instance.send_message(event_user.id,
                                                    terms_message_text,
                                                    reply_markup=keyboard)
                logging.info(f"Terms acceptance notification sent to user {event_user.id}.")
            except TelegramForbiddenError:
                logging.warning(
                    f"TermsAcceptance: Bot is blocked by user {event_user.id}.")
            except Exception as e_send:
                logging.error(
                    f"TermsAcceptance: Failed to notify user {event_user.id}: {type(e_send).__name__} - {e_send}",
                    exc_info=True)

            return
        return await handler(event, data)

