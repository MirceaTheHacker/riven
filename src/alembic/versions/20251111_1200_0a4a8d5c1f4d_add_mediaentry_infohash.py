"""add_mediaentry_infohash

Add `infohash` column to MediaEntry for tracking source torrents.

Revision ID: 0a4a8d5c1f4d
Revises: 6ad2a91a3d7f
Create Date: 2025-11-11 12:00:00
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0a4a8d5c1f4d"
down_revision: Union[str, None] = "6ad2a91a3d7f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add infohash column to MediaEntry if missing."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()
    mediaentry_exists = any(t.lower() == "mediaentry" for t in tables)
    if not mediaentry_exists:
        return

    # Preserve actual casing
    actual_table_name = next(
        (t for t in tables if t.lower() == "mediaentry"), "MediaEntry"
    )

    columns = {col["name"].lower() for col in inspector.get_columns(actual_table_name)}
    if "infohash" in columns:
        return

    op.add_column(
        actual_table_name,
        sa.Column("infohash", sa.String(), nullable=True),
    )
    op.create_index(
        op.f("ix_media_entry_infohash"),
        actual_table_name,
        ["infohash"],
        unique=False,
    )


def downgrade() -> None:
    """Remove infohash column (no-op if missing)."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()
    mediaentry_exists = any(t.lower() == "mediaentry" for t in tables)
    if not mediaentry_exists:
        return

    actual_table_name = next(
        (t for t in tables if t.lower() == "mediaentry"), "MediaEntry"
    )
    columns = {col["name"].lower() for col in inspector.get_columns(actual_table_name)}
    if "infohash" not in columns:
        return

    op.drop_index(op.f("ix_media_entry_infohash"), table_name=actual_table_name)
    op.drop_column(actual_table_name, "infohash")
