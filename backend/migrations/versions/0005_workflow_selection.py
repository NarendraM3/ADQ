"""Persist selected datasets independently for each onboarding workflow."""
from alembic import op
import sqlalchemy as sa

revision = "0005_workflow_selection"
down_revision = "0004_job_environment"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "workflow_selections",
        sa.Column("workflow_id", sa.String(length=36), primary_key=True),
        sa.Column("dataset_ids", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("source_connection_ids", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade():
    op.drop_table("workflow_selections")
