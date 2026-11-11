"""Persist column selections with workflow source selections."""
from alembic import op
import sqlalchemy as sa

revision = "0006_workflow_selected_columns"
down_revision = "0005_workflow_selection"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "workflow_selections",
        sa.Column("selected_columns", sa.JSON(), nullable=False, server_default="{}"),
    )


def downgrade():
    op.drop_column("workflow_selections", "selected_columns")
