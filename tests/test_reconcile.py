import importlib.util
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "reconcile.py"
SPEC = importlib.util.spec_from_file_location("reconcile", SCRIPT_PATH)
reconcile = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = reconcile
SPEC.loader.exec_module(reconcile)


def entry(
    transaction_hash: str,
    public_key: str,
    height: int,
    block_type: str,
    subject_public_key: str,
    previous_balance: int,
    balance: int,
    timestamp: int,
):
    return reconcile.Entry(
        transaction_hash,
        public_key,
        height,
        block_type,
        subject_public_key,
        previous_balance,
        balance,
        timestamp,
    )


class ReconcileTest(unittest.TestCase):
    def test_decodes_public_key_from_projected_address(self):
        # Given
        address = "atto://adqaxis7pdonb4rjvezvaybfntxreye5cyo26l6wctodfq34zxcwu3rtvdhn2"

        # When
        public_key = reconcile.public_key_from_address(address)

        # Then
        self.assertEqual(
            "E00BA25F78DCD0F229A9335060256CEF12609D161DAF2FD614DC32C37CCDC56A",
            public_key,
        )

    def test_formats_raw_amounts_without_losing_precision(self):
        # Given
        raw_amount = -50_000_000_900_000_000

        # When
        amount = reconcile.raw_to_amount(raw_amount)

        # Then
        self.assertEqual("-50000000.9", amount)
        self.assertEqual(raw_amount, reconcile.amount_to_raw(amount))

    def test_formats_small_signed_fiat_values_without_rounding_to_zero(self):
        # Given
        value = Decimal("-0.0001834")

        # When
        formatted = reconcile._format_csv_fiat(value)

        # Then
        self.assertEqual("-0.000183", formatted)

    def test_does_not_inherit_classification_from_existing_ledger_row(self):
        # Given
        treasury = "A" * 64
        receiver = "B" * 64
        send = entry("1" * 64, treasury, 2, "SEND", receiver, 60, 10, 1_000)
        receive = entry("2" * 64, receiver, 3, "RECEIVE", treasury, 5, 55, 1_500)
        logged_row = reconcile.LoggedRow(
            Path("log.csv"), 2, "1970-01-01", receive.hash, "rewards", "+0.00000005", "reward"
        )
        config = reconcile.Config(
            "UTC",
            (reconcile.SourceAccount("Treasury", treasury),),
            (),
            1,
            86_400,
            reconcile.Fraction("0.001"),
            True,
        )

        # When
        plan = reconcile.build_plan(config, [logged_row], [send])

        # Then
        self.assertEqual((), plan.rows)
        self.assertEqual((send,), plan.unknown_entries)

    def test_includes_setup_send_in_accounting_book(self):
        # Given
        treasury = "A" * 64
        recipient = "B" * 64
        setup = entry(
            "1" * 64,
            treasury,
            1,
            "SEND",
            recipient,
            60_000_001 * reconcile.RAW_PER_ATTO,
            60_000_000 * reconcile.RAW_PER_ATTO,
            1_000,
        )
        material = entry(
            "2" * 64,
            treasury,
            2,
            "SEND",
            recipient,
            60_000_000 * reconcile.RAW_PER_ATTO,
            10_000_000 * reconcile.RAW_PER_ATTO,
            2_000,
        )
        config = reconcile.Config(
            "UTC",
            (reconcile.SourceAccount("Treasury", treasury),),
            (reconcile.ClassificationRule("send", recipient, "dev-fund", "promo"),),
            reconcile.amount_to_raw("1000"),
            86_400,
            reconcile.Fraction("0.001"),
            True,
        )

        # When
        plan = reconcile.build_plan(config, [], [setup, material])

        # Then
        self.assertEqual((), plan.ignored_setup_hashes)
        self.assertEqual(2, len(plan.rows))
        self.assertEqual([setup.hash, material.hash], [row.values[1] for row in plan.rows])

    def test_records_both_internal_transfer_legs(self):
        # Given
        source = "A" * 64
        destination = "B" * 64
        send = entry(
            "1" * 64,
            source,
            1,
            "SEND",
            destination,
            100_000_000 * reconcile.RAW_PER_ATTO,
            50_000_000 * reconcile.RAW_PER_ATTO,
            1_000,
        )
        receive = entry(
            "2" * 64,
            destination,
            1,
            "RECEIVE",
            source,
            0,
            50_000_000 * reconcile.RAW_PER_ATTO,
            2_000,
        )
        config = reconcile.Config(
            "UTC",
            (
                reconcile.SourceAccount("Treasury 1", source),
                reconcile.SourceAccount("Treasury 2", destination),
            ),
            (),
            reconcile.amount_to_raw("1000"),
            86_400,
            reconcile.Fraction("0.001"),
            True,
        )

        # When
        plan = reconcile.build_plan(config, [], [send, receive])

        # Then
        self.assertEqual(2, len(plan.rows))
        self.assertEqual(
            ["internal-transfer", "internal-transfer"],
            [row.values[4] for row in plan.rows],
        )
        self.assertEqual(
            ["-50000000.0", "+50000000.0"],
            [row.values[5] for row in plan.rows],
        )
        self.assertEqual(
            ["Treasury 1 to Treasury 2", "Treasury 1 to Treasury 2"],
            [row.values[6] for row in plan.rows],
        )

    def test_creates_month_file_with_header_and_entry(self):
        # Given
        planned = reconcile.PlannedRow(
            timestamp=1,
            month="2026-01",
            values=(
                "2026-01-01",
                "A" * 64,
                "atto://treasury-4",
                "atto://staking",
                "network-incentives",
                "-50000000.0",
                "staking",
            ),
        )
        rate = reconcile.ExchangeRate(
            "2026-01-01",
            Decimal("0.0002"),
            Decimal("0.00025"),
        )

        # When
        with tempfile.TemporaryDirectory() as directory:
            paths = reconcile.write_planned_rows(Path(directory), [planned], [rate])
            content = paths[0].read_text(encoding="utf-8")

        # Then
        self.assertEqual(
            "date,hash,account,counterparty,allocation,amount,amount_eur,"
            "amount_usd,description\n"
            f"2026-01-01,{'A' * 64},atto://treasury-4,atto://staking,"
            "network-incentives,-50000000.0,-10000.00,-12500.00,staking\n",
            content,
        )

    def test_writes_separate_address_label_directory(self):
        # Given
        accounts = {
            "B": reconcile.AccountMetadata("B", "atto://b", "Treasury 2"),
            "A": reconcile.AccountMetadata("A", "atto://a", "Genesis"),
        }
        ledger_rows = [
            reconcile.PlannedRow(
                1,
                "2026-01",
                (
                    "2026-01-01",
                    "A" * 64,
                    "atto://a",
                    "atto://unlabeled",
                    "internal-transfer",
                    "-1.0",
                    "test",
                ),
            )
        ]

        # When
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "addresses.csv"
            reconcile.write_address_labels(path, accounts, ledger_rows)
            content = path.read_text(encoding="utf-8")

        # Then
        self.assertEqual(
            "address,label\natto://a,Genesis\n",
            content,
        )

    def test_normalizes_existing_row_from_network_entry(self):
        # Given
        transaction = entry(
            "A" * 64,
            "B" * 64,
            1,
            "RECEIVE",
            "C" * 64,
            0,
            50_000_000 * reconcile.RAW_PER_ATTO,
            86_400_000,
        )
        logged = reconcile.LoggedRow(
            Path("log/1970-01.csv"),
            2,
            "1970-01-01",
            transaction.hash,
            "community-rewards",
            "-50000000.9",
            "Reward Bot",
        )

        # When
        normalized = reconcile.normalize_logged_rows(
            [logged],
            {transaction.hash: transaction},
            "UTC",
            {
                transaction.public_key: reconcile.AccountMetadata(
                    transaction.public_key,
                    "atto://reward-bot",
                    "Reward Bot",
                )
            },
        )

        # Then
        self.assertEqual("1970-01-02", normalized[0].values[0])
        self.assertEqual("atto://reward-bot", normalized[0].values[2])
        self.assertEqual("+50000000.0", normalized[0].values[5])

    def test_selects_only_rows_owned_by_source_accounts(self):
        # Given
        treasury = "A" * 64
        external = "B" * 64
        treasury_entry = entry(
            "1" * 64, treasury, 1, "SEND", external, 10, 5, 1
        )
        external_entry = entry(
            "2" * 64, external, 1, "RECEIVE", treasury, 0, 5, 2
        )
        rows = [
            reconcile.LoggedRow(
                Path("log.csv"),
                2,
                "1970-01-01",
                treasury_entry.hash,
                "fund",
                "-0.000000005",
                "test",
            ),
            reconcile.LoggedRow(
                Path("log.csv"),
                3,
                "1970-01-01",
                external_entry.hash,
                "fund",
                "+0.000000005",
                "test",
            ),
        ]

        # When
        selected = reconcile.select_source_rows(
            rows,
            {
                treasury_entry.hash: treasury_entry,
                external_entry.hash: external_entry,
            },
            [reconcile.SourceAccount("Treasury 1", treasury)],
        )

        # Then
        self.assertEqual([treasury_entry.hash], [row.hash for row in selected])

    def test_calculates_whitepaper_allocations_from_net_spending(self):
        # Given
        config = reconcile.Config(
            "UTC",
            (),
            (),
            1,
            1,
            reconcile.Fraction(0),
            total_supply_raw=reconcile.amount_to_raw("100"),
            token_allocations=(
                reconcile.TokenAllocation(
                    "Development Fund",
                    reconcile.Fraction(60),
                    ("dev-fund",),
                ),
                reconcile.TokenAllocation(
                    "Founder Allocation",
                    reconcile.Fraction(40),
                    ("founder",),
                ),
            ),
        )
        rows = [
            reconcile.PlannedRow(
                1,
                "2026-01",
                ("2026-01-01", "A", "a", "b", "dev-fund", "-10.0", "work"),
            ),
            reconcile.PlannedRow(
                2,
                "2026-01",
                ("2026-01-02", "B", "a", "b", "dev-fund", "+2.0", "return"),
            ),
            reconcile.PlannedRow(
                3,
                "2026-01",
                (
                    "2026-01-03",
                    "C",
                    "a",
                    "b",
                    "internal-transfer",
                    "-50.0",
                    "rebalance",
                ),
            ),
        ]

        # When
        statuses = reconcile.calculate_allocation_statuses(config, rows)

        # Then
        self.assertEqual(reconcile.amount_to_raw("8"), statuses[0].spent_raw)
        self.assertEqual(reconcile.amount_to_raw("52"), statuses[0].remaining_raw)
        self.assertEqual(0, statuses[1].spent_raw)

    def test_writes_friendly_readme_with_allocation_status(self):
        # Given
        config = reconcile.Config(
            "UTC",
            (),
            (),
            1,
            1,
            reconcile.Fraction(0),
            total_supply_raw=reconcile.amount_to_raw("100"),
            token_allocations=(
                reconcile.TokenAllocation(
                    "Community Rewards",
                    reconcile.Fraction(100),
                    ("community-rewards",),
                ),
            ),
        )
        rows = [
            reconcile.PlannedRow(
                1,
                "2026-01",
                (
                    "2026-01-02",
                    "A",
                    "a",
                    "b",
                    "community-rewards",
                    "-25.0",
                    "reward",
                ),
            )
        ]
        statuses = reconcile.calculate_allocation_statuses(config, rows)
        rates = (
            reconcile.ExchangeRate(
                "2026-01-02",
                Decimal("2"),
                Decimal("3"),
            ),
        )
        current_rate = reconcile.CurrentExchangeRate(
            "2026-01-03", Decimal("4"), "2026-01-03", Decimal("5")
        )

        # When
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "README.md"
            reconcile.write_readme(
                path, config, rows, statuses, rates, current_rate
            )
            content = path.read_text(encoding="utf-8")

        # Then
        self.assertIn("# Atto Treasury Transparency", content)
        self.assertIn(
            "| Community Rewards | 100% | 100 | 25 "
            "| ≈ €50 / US$75 | 75 | ≈ €300 / US$375 | 25.00% |",
            content,
        )
        self.assertIn("activity through **2026-01-02**", content)
        self.assertIn("[**Exchange rates**](exchange-rates.csv)", content)

    def test_readme_shows_na_when_allocation_has_no_matched_spending_entry(self):
        # Given
        config = reconcile.Config(
            "UTC",
            (),
            (),
            1,
            1,
            reconcile.Fraction(0),
            total_supply_raw=reconcile.amount_to_raw("100"),
            token_allocations=(
                reconcile.TokenAllocation(
                    "Community Rewards",
                    reconcile.Fraction(50),
                    ("community-rewards",),
                ),
                reconcile.TokenAllocation(
                    "Founder Allocation",
                    reconcile.Fraction(50),
                    ("founder",),
                ),
            ),
        )
        rows = [
            reconcile.PlannedRow(
                1,
                "2026-01",
                (
                    "2026-01-02",
                    "A",
                    "a",
                    "b",
                    "community-rewards",
                    "-25.0",
                    "reward",
                ),
            )
        ]
        statuses = reconcile.calculate_allocation_statuses(config, rows)
        rates = (reconcile.ExchangeRate("2026-01-02", None, None),)
        current_rate = reconcile.CurrentExchangeRate(
            "2026-01-03", Decimal("4"), "2026-01-03", Decimal("5")
        )

        # When
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "README.md"
            reconcile.write_readme(
                path, config, rows, statuses, rates, current_rate
            )
            content = path.read_text(encoding="utf-8")

        # Then
        self.assertIn(
            "| Community Rewards | 50% | 50 | 25 | n/a | 25 ",
            content,
        )
        self.assertIn(
            "| Founder Allocation | 50% | 50 | 0 | n/a | 50 ",
            content,
        )

    def test_exchange_rate_cache_keeps_only_dates_used_by_ledger_entries(self):
        # Given
        current_rate = reconcile.CurrentExchangeRate(
            "2026-01-03", Decimal("4"), "2026-01-03", Decimal("5")
        )
        used = reconcile.ExchangeRate(
            "2026-01-02",
            Decimal("2"),
            Decimal("3"),
        )
        stale = reconcile.ExchangeRate(
            "2025-12-01",
            Decimal("1"),
            Decimal("1"),
        )
        internal = reconcile.ExchangeRate(
            "2026-01-03",
            Decimal("4"),
            Decimal("5"),
        )
        rows = [
            reconcile.PlannedRow(
                1,
                "2026-01",
                ("2026-01-02", "A", "a", "b", "dev-fund", "-25.0", "work"),
            ),
            reconcile.PlannedRow(
                2,
                "2026-01",
                (
                    "2026-01-03",
                    "B",
                    "a",
                    "b",
                    "internal-transfer",
                    "-10.0",
                    "rebalance",
                ),
            ),
        ]

        # When
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "exchange-rates.csv"
            reconcile.write_exchange_rates(path, [used, internal, stale])
            with patch.object(
                reconcile,
                "fetch_gatekeeper_current_exchange_rate",
                return_value=current_rate,
            ):
                rates, _, added, removed = reconcile.reconcile_exchange_rates(
                    path, rows, "https://gatekeeper.example/metrics", 365
                )
            reconcile.write_exchange_rates(path, rates)
            content = path.read_text(encoding="utf-8")

        # Then
        self.assertEqual((used, internal), rates)
        self.assertEqual(0, added)
        self.assertEqual(1, removed)
        self.assertTrue(content.startswith("date,eur,usd\n"))
        self.assertIn("2026-01-02", content)
        self.assertIn("2026-01-03", content)
        self.assertNotIn("2025-12-01", content)

    def test_retries_blank_rate_while_date_is_available_from_gatekeeper(self):
        # Given
        today = datetime.now(timezone.utc).date().isoformat()
        unavailable = reconcile.ExchangeRate(today, None, None)
        current_rate = reconcile.CurrentExchangeRate(
            today, Decimal("2"), today, Decimal("3")
        )
        rows = [
            reconcile.PlannedRow(
                1,
                today[:7],
                (today, "A", "a", "b", "dev-fund", "-25.0", "work"),
            )
        ]

        # When
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "exchange-rates.csv"
            reconcile.write_exchange_rates(path, [unavailable])
            with (
                patch.object(
                    reconcile,
                    "fetch_gatekeeper_price_history",
                    return_value={
                        "eur": {today: Decimal("2")},
                        "usd": {today: Decimal("3")},
                    },
                ),
                patch.object(
                    reconcile,
                    "fetch_gatekeeper_current_exchange_rate",
                    return_value=current_rate,
                ),
            ):
                rates, _, refreshed, removed = reconcile.reconcile_exchange_rates(
                    path, rows, "https://gatekeeper.example/metrics", 365
                )

        # Then
        self.assertEqual(
            (reconcile.ExchangeRate(today, Decimal("2"), Decimal("3")),),
            rates,
        )
        self.assertEqual(1, refreshed)
        self.assertEqual(0, removed)

    def test_does_not_substitute_a_price_from_another_date(self):
        # Given
        prices = {
            "2025-08-13": Decimal("0.0002"),
            "2025-08-14": Decimal("0.0003"),
        }

        # When
        match = reconcile._price_for_date(prices, "2024-11-23")

        # Then
        self.assertIsNone(match)

    def test_leaves_per_entry_fiat_values_blank_without_an_exact_rate(self):
        # Given
        row = reconcile.PlannedRow(
            1,
            "2024-11",
            ("2024-11-23", "A", "a", "b", "dev-fund", "-25.0", "work"),
        )
        unavailable = reconcile.ExchangeRate("2024-11-23", None, None)

        # When
        values = reconcile._ledger_csv_values(
            row, {unavailable.date: unavailable}
        )

        # Then
        self.assertEqual("", values[6])
        self.assertEqual("", values[7])


if __name__ == "__main__":
    unittest.main()
