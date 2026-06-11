"""fetch_task params gin index

Revision ID: b9856ff476b1
Revises: b531a949fc9b
Create Date: 2026-06-11 17:33:22.245471

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b9856ff476b1'
down_revision: Union[str, Sequence[str], None] = 'b531a949fc9b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """ensure_quarter 的 params @> {...} 容器查询走索引（fetch_task 只增不删）。"""
    op.create_index(
        "ix_fetch_task_params_gin",
        "fetch_task",
        ["params"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix_fetch_task_params_gin", table_name="fetch_task")
