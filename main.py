# Retrieve tokens from: https://developers.facebook.com/tools/explorer/
from typing import Optional, Mapping
import contextlib
import asyncio
import os
import more_itertools as itertools
import logging
import collections

import aiohttp

ACCOUNT_ID = os.environ["AD_ACCOUNT_ID"]
ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
BATCH_SIZE = 10_000

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class MetaClientError(Exception):
    pass


class MetaClient:
    def __init__(self, account_id, access_token, poll_time=10):
        self.account_id = account_id
        self.access_token = access_token
        self.poll_time = poll_time

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            base_url="https://graph.facebook.com",
            headers={"Host": "graph.facebook.com"},
        )
        return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    @contextlib.asynccontextmanager
    async def _post(self, url, params, json={}):
        async with self.session.post(url, params=params, json=json) as response:
            yield response

    @contextlib.asynccontextmanager
    async def _delete(self, url, params, json):
        async with self.session.delete(url, params=params, json=json) as response:
            yield response

    @contextlib.asynccontextmanager
    async def _get(self, url, params):
        async with self.session.get(url, params=params) as response:
            yield response

    async def get_audience(self, audience_name: str):
        # https://developers.facebook.com/docs/marketing-api/reference/ad-account/customaudiences/#fields
        async with self._get(
            f"/v21.0/act_{self.account_id}/customaudiences",
            params={
                "access_token": self.access_token,
                "fields": ["id", "name"],
                # Filtering on `name` doesn't appear to be supported
                # "filtering": json.dumps(
                #     [{"field": "name", "operator": "EQUAL", "value": audience_name}]
                # ),
            },
        ) as response:
            if response.status == 200:
                # TODO: Page through response
                # https://developers.facebook.com/docs/graph-api/results
                json_body = await response.json()
                next_page = json_body["paging"].get("next")
                audience_id = next(
                    (
                        audience["id"]
                        for audience in json_body["data"]
                        if audience["name"] == audience_name
                    ),
                    None,
                )
                logger.info(f"Retrieved audience. ({audience_id=}, {next_page=})")
                return audience_id
            else:
                body = await response.text()
                logger.warning(f"Failed to get audience. ({response.status=}, {body=})")

    async def create_audience(self, audience_name: str):
        async with self._post(
            f"/v21.0/act_{self.account_id}/customaudiences",
            params={
                "name": audience_name,
                "subtype": "CUSTOM",
                "description": "My first audience",
                "customer_file_source": "USER_PROVIDED_ONLY",
                "access_token": self.access_token,
            },
        ) as response:
            if response.status == 200:
                json_body = await response.json()
                return json_body["data"]["id"]
            else:
                body = await response.text()
                logger.warning(
                    f"Failed to create audience. ({response.status=}, {body=})"
                )

    async def add_users(self, audience_id, members, session: Optional[Mapping] = None):
        # https://developers.facebook.com/docs/marketing-api/reference/custom-audience/users/#Updating
        payload = {
            "payload": {
                "schema": "EMAIL_SHA256",
                "data": [member["email"] for member in members],
            }
        }
        if session:
            payload.update({"session": session})

        async with self._post(
            f"/v21.0/{audience_id}/users",
            params={"access_token": self.access_token},
            json=payload,
        ) as response:
            if response.status == 200:
                json_body = await response.json()
                assert json_body["num_invalid_entries"] == 0
                logger.info(
                    f"Submitted job to add user to audience. ({session=}, {json_body=})"
                )
            else:
                body = await response.text()
                logger.error(
                    f"Failed to add users to upload job. ({response.status=}, {body=})"
                )
                raise MetaClientError("Failed to add users to upload job")

    async def remove_users(
        self, audience_id, members, session: Optional[Mapping] = None
    ):
        # https://developers.facebook.com/docs/marketing-api/reference/custom-audience/users/#Deleting
        payload = {
            "payload": {
                "schema": "EMAIL_SHA256",
                "data": [member["email"] for member in members],
            }
        }
        if session:
            payload.update({"session": session})

        async with self._delete(
            f"/v21.0/{audience_id}/users",
            params={"access_token": self.access_token},
            json=payload,
        ) as response:
            if response.status == 200:
                json_body = await response.json()
                assert json_body["num_invalid_entries"] == 0
                logger.info(
                    f"Submitted job to remove user from audience. ({session=}, {json_body=})"
                )
                return json_body["session_id"]
            else:
                body = await response.text()
                logger.error(
                    f"Failed to remove users from audience. ({response.status=}, {body=})"
                )
                raise MetaClientError("Failed to remove users from audience")

    async def get_session_info(self, audience_id, session_id):
        # https://developers.facebook.com/docs/marketing-api/reference/custom-audience/sessions/
        async with self._get(
            f"/v21.0/{audience_id}/sessions",
            params={
                "access_token": self.access_token,
                "session_id": session_id,
            },
        ) as response:
            if response.status == 200:
                json_body = await response.json()
                logger.info(
                    f"Retrieved session info. ({response.status=}, {json_body=})"
                )
                return json_body["data"][0]
            else:
                body = await response.text()
                logger.error(
                    f"Failed to get session info. ({response.status=}, {body=})"
                )
                raise MetaClientError("Failed to get session info")

    async def wait_for_session_complete(self, audience_id, sessions):
        while sessions:
            session = sessions.pop()
            session_id = session["session_id"]
            session_info = await self.get_session_info(audience_id, session_id)
            stage = session_info["stage"]
            if stage in ("processing", "uploading"):
                logger.info(f"Session is still active. ({session_id=}, {stage=})")
                await asyncio.sleep(self.poll_time)
                sessions.append(session)
            else:
                session_id = session["session_id"]
                logger.info(f"Session has finished. ({session_id=}, {stage=})")

    async def remove_audience_members(self, audience_id, members):
        if len(members) <= BATCH_SIZE:
            await self.remove_users(audience_id, members)
        else:
            sessions = collections.deque()
            for batch_number, batch in enumerate(
                itertools.batched(members, BATCH_SIZE)
            ):
                session = {
                    "session_id": abs(hash(audience_id)) + batch_number,
                    "estimated_num_total": len(members),
                    "batch_seq": batch_number + 1,
                    "last_batch_flag": (batch_number + 1) * BATCH_SIZE >= len(members),
                }
                await self.remove_users(audience_id, batch, session)
                sessions.append(session)

            await self.wait_for_session_complete(audience_id, sessions)

        logger.info("Removed audience members")

    async def add_audience_members(self, audience_id, members):
        if len(members) <= BATCH_SIZE:
            await self.add_users(audience_id, members)
        else:
            sessions = collections.deque()
            for batch_number, batch in enumerate(
                itertools.batched(members, BATCH_SIZE)
            ):
                session = {
                    "session_id": abs(hash(audience_id)) + batch_number,
                    "estimated_num_total": len(members),
                    "batch_seq": batch_number + 1,
                    "last_batch_flag": (batch_number + 1) * BATCH_SIZE >= len(members),
                }
                await self.add_users(audience_id, batch, session)
                sessions.append(session)

            await self.wait_for_session_complete(audience_id, sessions)
            logger.info("Added audience members")


async def main():
    client = MetaClient(ACCOUNT_ID, ACCESS_TOKEN)
    audience_name = "My First Audience"
    members = [
        {"email": "2114de26f9f2fadca2877f67c2a0a81e986bbc88490fc3c51dc2f3af0eaaba09"}
    ]
    async with client:
        audience_id = await client.get_audience(audience_name)

        if not audience_id:
            audience_id = await client.create_audience(audience_name)

        await client.remove_audience_members(audience_id, members)

        await client.add_audience_members(audience_id, members)


if __name__ == "__main__":
    asyncio.run(main())
