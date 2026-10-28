"""一次性维护脚本：把已经发出去的 Key 都放开到指定的模型白名单。

背景：一开始按商品分模型（比如 deepseek 商品只放 deepseek、kimi 商品只放 kimi），
后来改成「不管买哪个商品，Key 都能用这几个模型」。已经建好的令牌是创建时写死
模型白名单的，**改配置只影响新开的 Key**，老 Key 得回头改一遍。

用法（默认只预览，不写任何东西）：
    .venv/bin/python scripts/open_models.py --models "deepseek-chat,kimi-k2"
    .venv/bin/python scripts/open_models.py --models "deepseek-chat,kimi-k2" --apply

默认取**并集**：只把缺的模型加进去，不动令牌原有的其它模型 —— 少写一个模型名
就等于把一个已经卖出去的权限收回去，那比多给还麻烦。想强行对齐用 --exact。

不带 --models 时用后台「设置」里的全局白名单（没有就用 .env / 内置默认值），
也就是「新开的 Key 会拿到的那一组」。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import resolve_model_limits, settings
from app.db import Database
from app.upstream import NewApiClient, UpstreamError, normalize_model_limits


def split_models(raw: str) -> list[str]:
    """把逗号 / 中文逗号 / 空白分隔的模型名拆成列表（去空去重、保序）。"""
    text = normalize_model_limits(raw)
    return [name for name in text.split(",") if name]


def merge_models(current: str, wanted: list[str]) -> str:
    """当前 + 目标 的并集：只加不减。"""
    merged = split_models(current)
    for name in wanted:
        if name not in merged:
            merged.append(name)
    return ",".join(merged)


def current_models(token: dict) -> str:
    """令牌实际生效的白名单；没启用 model_limits 就是「不限」。"""
    if not token.get("model_limits_enabled"):
        return ""
    return normalize_model_limits(str(token.get("model_limits") or ""))


async def resolve_target(db: Database, raw: str) -> tuple[str, str]:
    """目标白名单 + 它从哪来。"""
    if raw.strip():
        return normalize_model_limits(raw), "--models"
    item_models, overrides = await db.model_settings()
    limits, source = resolve_model_limits("*", item_models, overrides)
    return normalize_model_limits(limits), source


async def run(args: argparse.Namespace) -> int:
    db = Database(settings.db_path)
    await db.init()
    wanted_text, source = await resolve_target(db, args.models)
    wanted = split_models(wanted_text)
    if not wanted:
        print("目标模型白名单是空的 —— 空串在 new-api 里表示「不限模型」，"
              "不做这种操作，请用 --models 明确写出来。")
        return 2

    print(f"目标模型：{wanted_text}（来自 {source}）")
    print("模式：", "并集（只加不减）" if not args.exact else "替换（--exact）")
    print("动作：", "真改" if args.apply else "只预览（加 --apply 才会写）")

    client = NewApiClient(
        settings.newapi_base_url,
        access_token=settings.newapi_session,
        username=settings.newapi_username,
        password=settings.newapi_password,
        group=settings.newapi_group,
        quota_per_unit=settings.newapi_quota_per_unit,
        user_id=settings.newapi_user_id,
    )
    changed = skipped = failed = 0
    try:
        tokens = await client.list_all_tokens(page_size=100)
        print(f"共 {len(tokens)} 个令牌")
        for token in tokens:
            token_id = str(token.get("id") or "").strip()
            name = str(token.get("name") or "-")
            if not token_id:
                continue
            if args.keyword and args.keyword not in name:
                continue
            before = current_models(token)
            if not before:
                # 不限模型 = 本来就已经全开，没必要动（动了反而收窄）
                skipped += 1
                print(f"  跳过 {token_id} {name}：不限模型")
                continue
            after = wanted_text if args.exact else merge_models(before, wanted)
            if after == before:
                skipped += 1
                print(f"  跳过 {token_id} {name}：已经是 {before}")
                continue
            print(f"  {'改' if args.apply else '将改'} {token_id} {name}：{before} → {after}")
            if not args.apply:
                changed += 1
                continue
            try:
                await client.set_token_models(token_id, after)
                changed += 1
            except UpstreamError as exc:
                failed += 1
                print(f"    失败：{exc}")
    finally:
        await client.close()

    print(f"\n完成：{'计划/已改' if not args.apply else '已改'} {changed} 个，"
          f"跳过 {skipped} 个，失败 {failed} 个")
    if not args.apply and changed:
        print("确认无误后加 --apply 再跑一次。")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="把已有令牌的模型白名单放开到指定模型")
    parser.add_argument("--models", default="",
                        help="逗号分隔的模型名；不填用后台设置里的全局白名单")
    parser.add_argument("--exact", action="store_true",
                        help="用 --models 直接替换（默认是并集，只加不减）")
    parser.add_argument("--keyword", default="",
                        help="只处理令牌名包含这个关键字的令牌")
    parser.add_argument("--apply", action="store_true", help="真的写回 new-api")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
