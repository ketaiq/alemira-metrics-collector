import pandas as pd


def process():
    df = pd.read_csv("day-1.csv")
    print(len(df))


def main():
    process()


if __name__ == "__main__":
    main()
