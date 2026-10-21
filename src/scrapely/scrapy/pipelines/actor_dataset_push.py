# SPDX-License-Identifier: Apache-2.0
# Derived from apify-sdk-python (src/apify/scrapy/pipelines/actor_dataset_push.py),
# Copyright Apify Technologies s.r.o., licensed under the Apache License 2.0.
# Modified by TeraShift GmbH. See LICENSE-APACHE and NOTICE.
from __future__ import annotations

from typing import TYPE_CHECKING

from itemadapter.adapter import ItemAdapter

from scrapely import Actor

if TYPE_CHECKING:
    from scrapy import Item


class ActorDatasetPushPipeline:
    """A Scrapy pipeline for pushing items to an Actor's default dataset.

    This pipeline is designed to be enabled only when the Scrapy project is run as an Actor.
    """

    async def process_item(
        self,
        item: Item,
    ) -> Item:
        """Pushes the provided Scrapy item to the Actor's default dataset."""
        item_dict = ItemAdapter(item).asdict()
        await Actor.push_data(item_dict)
        return item
