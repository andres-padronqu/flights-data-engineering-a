from __future__ import annotations

from sqlalchemy import BigInteger, Float, ForeignKey, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Airline(Base):
    __tablename__ = "airlines"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airline: Mapped[str] = mapped_column(String(255), nullable=False)

    flights: Mapped[list["Flight"]] = relationship(
        back_populates="airline_ref",
        foreign_keys="Flight.airline",
    )


class Airport(Base):
    __tablename__ = "airports"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airport: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[str] = mapped_column(String(10), nullable=False)
    country: Mapped[str | None] = mapped_column(String(100), nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)

    departing_flights: Mapped[list["Flight"]] = relationship(
        back_populates="origin_airport_ref",
        foreign_keys="Flight.origin_airport",
    )
    arriving_flights: Mapped[list["Flight"]] = relationship(
        back_populates="destination_airport_ref",
        foreign_keys="Flight.destination_airport",
    )


class Flight(Base):
    __tablename__ = "flights"

    flight_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    year: Mapped[int] = mapped_column(BigInteger, nullable=False)
    month: Mapped[int] = mapped_column(BigInteger, nullable=False)
    day: Mapped[int] = mapped_column(BigInteger, nullable=False)
    day_of_week: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    airline: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("airlines.iata_code"),
        nullable=False,
    )
    flight_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    tail_number: Mapped[str | None] = mapped_column(String(20), nullable=True)

    origin_airport: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("airports.iata_code"),
        nullable=False,
    )
    destination_airport: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("airports.iata_code"),
        nullable=False,
    )

    scheduled_departure: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    departure_time: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    departure_delay: Mapped[float | None] = mapped_column(Float, nullable=True)

    scheduled_arrival: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    arrival_time: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    arrival_delay: Mapped[float | None] = mapped_column(Float, nullable=True)

    scheduled_time: Mapped[float | None] = mapped_column(Float, nullable=True)
    elapsed_time: Mapped[float | None] = mapped_column(Float, nullable=True)
    air_time: Mapped[float | None] = mapped_column(Float, nullable=True)
    distance: Mapped[float | None] = mapped_column(Float, nullable=True)

    wheels_off: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    taxi_out: Mapped[float | None] = mapped_column(Float, nullable=True)
    wheels_on: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    taxi_in: Mapped[float | None] = mapped_column(Float, nullable=True)

    diverted: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    cancelled: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    cancellation_reason: Mapped[str | None] = mapped_column(String(5), nullable=True)

    air_system_delay: Mapped[float | None] = mapped_column(Float, nullable=True)
    security_delay: Mapped[float | None] = mapped_column(Float, nullable=True)
    airline_delay: Mapped[float | None] = mapped_column(Float, nullable=True)
    late_aircraft_delay: Mapped[float | None] = mapped_column(Float, nullable=True)
    weather_delay: Mapped[float | None] = mapped_column(Float, nullable=True)

    airline_ref: Mapped["Airline"] = relationship(
        back_populates="flights",
        foreign_keys=[airline],
    )
    origin_airport_ref: Mapped["Airport"] = relationship(
        back_populates="departing_flights",
        foreign_keys=[origin_airport],
    )
    destination_airport_ref: Mapped["Airport"] = relationship(
        back_populates="arriving_flights",
        foreign_keys=[destination_airport],
    )