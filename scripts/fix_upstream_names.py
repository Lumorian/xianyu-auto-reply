"""一次性维护脚本：把上游已经建错的「买家#订单号」改成真昵称。

背景：以前付款卡片拿不到昵称、补名又只查本地聊天记录，新买家第一单就被记成
「买家」。本地 orders / messages 已经能自动修（每次启动扫一遍），但
**new-api 的令牌名和卡密站的 label 是创建时写死的**，只能回头改。

用法（默认只预览，不写任何东西）：
    .venv/bin/python scripts/fix_upstream_names.py             # 预览
    .venv/bin/python scripts/fix_upstream_names.py --apply     # 真改

匹配方式：拿本地 orders 里「订单号 -> 已修复的真昵称」，拼成「昵称#订单号」，
和上游现有的名字逐个比对，只改不一样的。默认**只改占位名**
（买家 / 等待你发货 / 交易消息…）；更早版本的旧格式
（「闲鱼自动发货 1.00元 #订单号」）不算错，要统一再加 --legacy-labels。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.codec import is_placeholder_name
from app.config import settings
from app.db import Database
from app.upstream import (
    CardShopClient,
    NewApiClient,
    UpstreamError,
    build_token_name,
    order_id_from_label,
)

#: 卡密站 label 的旧格式前缀，和「占位名」区分开：格式旧但内容是真实的。
LEGACY_LABEL_PREFIX = "闲鱼自动发货"


async def load_names(db: Database, account_id: int) -> dict[str, str]:
    """订单号 -> 真昵称（只收本地已经不是占位名的）。"""
    rows = await db.fetch_all(
        "SELECT order_id, buyer_name FROM orders"
        " WHERE account_id = ? AND order_id IS NOT NULL AND order_id != ''",
        (account_id,),
    )
    names: dict[str, str] = {}
    for row in rows:
        order_id = str(row["order_id"])
        name = str(row["buyer_name"] or "").strip()
        if name and not is_placeholder_name(name):
            names[order_id] = name
    return names


async def fix_tokens(client: NewApiClient, names: dict[str, str], apply: bool) -> tuple[int, int]:
    """改 new-api 令牌名；返回 (改动数, 失败数)。"""
    changed = failed = 0
    for item in await client.list_tokens(page_size=100):
        old = str(item.get("name") or "")
        order_id = order_id_from_label(old)
        if not order_id or order_id not in names:
            continue
        want = build_token_name(names[order_id], order_id, prefix=settings.newapi_token_prefix or "")
        if want == old:
            continue

        print(f"  令牌 {item.get('id')!s:>4}  {old}  ->  {want}")
        if not apply:
            changed += 1
            continue
        try:
            await client.rename_token(str(item.get("id")), want)
        except UpstreamError as exc:
            print(f"    ✗ {exc}")
            failed += 1
            continue
        changed += 1
    return changed, failed


async def fix_card_labels(names: dict[str, str], apply: bool, legacy: bool) -> tuple[int, int]:
    """改卡密站 label；返回 (改动数, 失败数)。"""
    shop = CardShopClient(
        settings.cardshop_base_url,
        admin_token=settings.cardshop_admin_token,
        username=settings.cardshop_username,
        password=settings.cardshop_password,
        groups=settings.cardshop_groups,
    )
    changed = failed = 0
    try:
        cards = await shop.list_cards(page_size=200)
        if apply and not await shop.can_rename_card():
            print("  - 这个卡密站没有改标签的接口（PATCH 返回 405），label 只能手工处理")
            return 0, 0
        for item in cards:
            old = str(item.get("label") or "")
            order_id = order_id_from_label(old)
            if not order_id or order_id not in names:
                continue
            want = f"{names[order_id]}#{order_id}"[:100]
            if want == old:
                continue
            looks_legacy = old.startswith(LEGACY_LABEL_PREFIX)
            if looks_legacy and not legacy:
                continue
            if not looks_legacy and not is_placeholder_name(old.split("#")[0]):
                continue

            print(f"  卡密 {item.get('id')!s:>3}  {old}  ->  {want}")
            if not apply:
                changed += 1
                continue
            try:
                await shop.rename_card(item.get("id"), want)
            except UpstreamError as exc:
                print(f"    ✗ {exc}")
                failed += 1
                continue
            changed += 1
        return changed, failed
    finally:
        await shop.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="修正上游里「买家#订单号」这类占位令牌名 / 卡密标签")
    parser.add_argument("--apply", action="store_true", help="真的写回去；不加就是预览")
    parser.add_argument("--account", type=int, default=1, help="账号 id，默认 1")
    parser.add_argument("--legacy-labels", action="store_true",
                        help="连「闲鱼自动发货 xx元 #订单号」这类旧格式标签一起统一")
    args = parser.parse_args()

    async def run() -> None:
        db = Database(settings.db_path)
        await db.init()
        names = await load_names(db, args.account)
        print(f"本地可用的真名：{len(names)} 单；模式：{'写入' if args.apply else '预览'}")

        client = NewApiClient(
            settings.newapi_base_url,
            access_token=settings.newapi_session,
            group=settings.newapi_group,
            quota_per_unit=settings.newapi_quota_per_unit,
            user_id=settings.newapi_user_id,
            model_limits=settings.newapi_model_limits,
        )
        try:
            await client.ensure_user_id()
            print("\n[new-api 令牌名]")
            token_done, token_failed = await fix_tokens(client, names, args.apply)
        finally:
            await client.close()

        print("\n[卡密站 label]")
        label_done, label_failed = await fix_card_labels(names, args.apply, args.legacy_labels)

        print(f"\n合计：令牌 {token_done} 条（失败 {token_failed}），"
              f"卡密 {label_done} 条（失败 {label_failed}）。")
        if not args.apply:
            print("以上是预览，加 --apply 才会真正写回去。")

    asyncio.run(run())


if __name__ == "__main__":
    main()
