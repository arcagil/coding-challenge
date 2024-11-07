"""initial migration

Revision ID: initial
Revises: 
Create Date: 2024-03-19 10:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "customerdailystats",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("customer_id", sa.String(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("successful_requests", sa.Integer(), nullable=False),
        sa.Column("failed_requests", sa.Integer(), nullable=False),
        sa.Column("uptime_percentage", sa.Float(), nullable=False),
        sa.Column("avg_latency", sa.Float(), nullable=False),
        sa.Column("median_latency", sa.Float(), nullable=False),
        sa.Column("p99_latency", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_customerdailystats_customer_id_date"),
        "customerdailystats",
        ["customer_id", "date"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_customerdailystats_customer_id_date"), table_name="customerdailystats"
    )
    op.drop_table("customerdailystats")
