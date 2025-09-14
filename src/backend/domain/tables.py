from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    MetaData,
    String,
    Table,
    func,
)

__all__ = ["dim_user", "fact_activity", "user_state_daily", "metadata"]

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=NAMING_CONVENTION)

GROWTH_STATE_VALUES = (
    "NEW",
    "CURRENT",
    "REACTIVATED",
    "RESURRECTED",
    "AT_RISK_WAU",
    "AT_RISK_MAU",
    "DORMANT",
)
state_enum = Enum(*GROWTH_STATE_VALUES, name="growth_state")


dim_user = Table(
    "dim_user",
    metadata,
    Column("user_id", String(64), primary_key=True),
    Column("first_seen_date", Date, nullable=False),
)


fact_activity = Table(
    "fact_activity",
    metadata,
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column(
        "user_id",
        String(64),
        ForeignKey("dim_user.user_id", ondelete="CASCADE"),
        nullable=False,
    ),
)


user_state_daily = Table(
    "user_state_daily",
    metadata,
    Column("as_of_date", Date, primary_key=True, nullable=False),
    Column(
        "user_id",
        String(64),
        ForeignKey("dim_user.user_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    ),
    Column("state", state_enum, nullable=False),
    Column("last_active_date", Date),
    Column("computed_at", DateTime, nullable=False, server_default=func.now()),
    CheckConstraint(
        "last_active_date IS NULL OR last_active_date <= as_of_date",
        name="last_active_le_as_of",
    ),
)
