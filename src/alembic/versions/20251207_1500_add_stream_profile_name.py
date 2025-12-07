"""add_stream_profile_name

Add `profile_name` column to Stream so multi-profile tags persist across service hops.

Revision ID: add_stream_profile_name
Revises: 0a4a8d5c1f4d
Create Date: 2025-12-07 15:00:00
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "add_stream_profile_name"
down_revision: Union[str, None] = "0a4a8d5c1f4d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add profile_name column to Stream if missing."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()
    stream_exists = any(t.lower() == "stream" for t in tables)
    if not stream_exists:
        return

    actual_table_name = next((t for t in tables if t.lower() == "stream"), "Stream")

    columns = {col["name"].lower() for col in inspector.get_columns(actual_table_name)}
    if "profile_name" in columns:
        return

    op.add_column(
        actual_table_name,
        sa.Column("profile_name", sa.String(), nullable=True),
    )
    op.create_index(
        op.f("ix_stream_profile_name"),
        actual_table_name,
        ["profile_name"],
        unique=False,
    )


def downgrade() -> None:
    """Remove profile_name column (no-op if missing)."""
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()
    stream_exists = any(t.lower() == "stream" for t in tables)
    if not stream_exists:
        return

    actual_table_name = next((t for t in tables if t.lower() == "stream"), "Stream")

    columns = {col["name"].lower() for col in inspector.get_columns(actual_table_name)}
    if "profile_name" not in columns:
        return

    op.drop_index(op.f("ix_stream_profile_name"), table_name=actual_table_name)
    op.drop_column(actual_table_name, "profile_name")
