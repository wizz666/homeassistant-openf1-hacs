"""Config flow for OpenF1."""
from __future__ import annotations

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import (
    CONF_FAVORITE_DRIVER,
    CONF_POLL_INTERVAL,
    DEFAULT_FAVORITE_DRIVER,
    DEFAULT_POLL_INTERVAL,
    DOMAIN,
)


class OpenF1ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        if user_input is not None:
            return self.async_create_entry(title="OpenF1", data=user_input)

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Optional(
                    CONF_POLL_INTERVAL,
                    default=DEFAULT_POLL_INTERVAL,
                ): selector.selector({
                    "number": {
                        "min": 30,
                        "max": 300,
                        "step": 10,
                        "mode": "slider",
                        "unit_of_measurement": "s",
                    }
                }),
                vol.Optional(
                    CONF_FAVORITE_DRIVER,
                    default=DEFAULT_FAVORITE_DRIVER,
                ): selector.selector({
                    "text": {}
                }),
            }),
            description_placeholders={
                "api_url": "api.openf1.org",
            },
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return OpenF1OptionsFlow(config_entry)


class OpenF1OptionsFlow(config_entries.OptionsFlow):
    def __init__(self, entry) -> None:
        self._entry = entry

    async def async_step_init(self, user_input=None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        cur = {**self._entry.data, **self._entry.options}

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({
                vol.Optional(
                    CONF_POLL_INTERVAL,
                    default=int(cur.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)),
                ): selector.selector({
                    "number": {
                        "min": 30,
                        "max": 300,
                        "step": 10,
                        "mode": "slider",
                        "unit_of_measurement": "s",
                    }
                }),
                vol.Optional(
                    CONF_FAVORITE_DRIVER,
                    default=cur.get(CONF_FAVORITE_DRIVER, DEFAULT_FAVORITE_DRIVER),
                ): selector.selector({
                    "text": {}
                }),
            }),
        )
