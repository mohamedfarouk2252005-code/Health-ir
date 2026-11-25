import argparse
from pathlib import Path

from src.spark_session import create_spark_session
from src.config import DEFAULT_DATA_DIR
from src.pipeline import (
    build_pipeline,
    new_session,
    TABLE_MENU,
    show_table,
    show_all_tables,
    save_calculation_outputs,
    save_query_outputs,
    run_term_query,
    run_phrase_query,
    run_boolean_query,
    run_assigned_queries
)


BANNER = """
========================================
 INFORMATION RETRIEVAL SYSTEM (PySpark)
========================================"""

MENU = """
------------- MAIN MENU -------------
 1) Term search
 2) Phrase query
 3) Boolean query  (AND / AND NOT)
 4) Show tables (index, TF, DF/IDF, TF-IDF, ...)
 5) Run the assigned queries
 6) Change dataset folder
 0) Exit
-------------------------------------"""


def ask_folder(default: Path) -> Path:
    raw = input(f"\nDataset folder [{default}]: ").strip().strip('"').strip("'")
    return Path(raw) if raw else default


def load_dataset(spark, folder):
    """Build everything for a folder; returns (ctx, session) or None."""

    print(f"\nReading documents from: {folder}")

    try:
        ctx = build_pipeline(spark, folder)
    except (FileNotFoundError, ValueError) as error:
        print(f"\nERROR: {error}")
        return None

    print(
        f"Loaded {ctx['num_documents']} documents "
        f"({', '.join(ctx['document_ids'])}) "
        f"- {ctx['num_terms']} unique terms."
    )

    save_calculation_outputs(ctx)

    return ctx, new_session()


def tables_menu(ctx):
    print("\nWhich table?")

    for number, (title, _) in enumerate(TABLE_MENU, start=1):
        print(f" {number}) {title}")

    print(f" {len(TABLE_MENU) + 1}) All tables")

    choice = input("Choice: ").strip()

    if choice == str(len(TABLE_MENU) + 1):
        show_all_tables(ctx)
    elif choice.isdigit() and 1 <= int(choice) <= len(TABLE_MENU):
        show_table(ctx, int(choice))
    else:
        print("Invalid choice.")


def main():
    parser = argparse.ArgumentParser(description="PySpark IR system")
    parser.add_argument("--folder", help="folder containing .txt documents")
    parser.add_argument(
        "--batch", action="store_true",
        help="run the assigned queries and exit (no menu)"
    )
    args = parser.parse_args()

    print(BANNER)

    spark = create_spark_session()

    try:
        state = None
        folder = Path(args.folder) if args.folder else ask_folder(DEFAULT_DATA_DIR)

        while state is None:
            state = load_dataset(spark, folder)
            if state is None:
                folder = ask_folder(DEFAULT_DATA_DIR)

        ctx, session = state

        if args.batch:
            run_assigned_queries(ctx, session)
            save_query_outputs(session)
            return

        while True:

            print(MENU)
            choice = input("Choice: ").strip()

            try:
                if choice == "1":
                    run_term_query(ctx, session, input("Term: ").strip())
                    save_query_outputs(session)

                elif choice == "2":
                    run_phrase_query(ctx, session, input("Phrase: ").strip())
                    save_query_outputs(session)

                elif choice == "3":
                    run_boolean_query(
                        ctx, session,
                        input("Boolean query (e.g. a b AND NOT c d): ").strip()
                    )
                    save_query_outputs(session)

                elif choice == "4":
                    tables_menu(ctx)

                elif choice == "5":
                    run_assigned_queries(ctx, session)
                    save_query_outputs(session)

                elif choice == "6":
                    new_state = load_dataset(spark, ask_folder(ctx["folder"]))
                    if new_state:
                        ctx, session = new_state

                elif choice == "0":
                    break

                else:
                    print("Invalid choice.")

            except ValueError as error:
                print(f"\nERROR: {error}")

    except (EOFError, KeyboardInterrupt):
        print("\nInterrupted.")

    finally:
        spark.stop()
        print("\nSpark session stopped. Goodbye.")


if __name__ == "__main__":
    main()
