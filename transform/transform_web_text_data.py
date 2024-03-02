import re
import pandas as pd


def transform_web_text_data(df: pd.DataFrame) -> pd.DataFrame:
    # clean the name of authors
    df["author"] = df["author"].str.replace(" - detikInet", "")

    # clean the title and the body
    df["title"] = df["title"].str.strip()
    df["body"] = df["body"].str.strip()

    # create a function to clean the acquired text
    def cleaning_text(text: str) -> str:
        # remove whitespace
        text_wo_whitespace = re.sub(r"\s+", " ", text)
        # add space after period
        proper_sentences = re.sub(r"\.([a-zA-Z]+)\b", r". \1", text_wo_whitespace)
        # split paragraph into sentences
        sentences = proper_sentences.split(". ")
        # filter irrelevant sentences
        filtered_sentences = filter(
            lambda sentence: "ADVERTISEMENT" not in sentence
            and "Baca juga:" not in sentence
            and "Simak Video" not in sentence,
            sentences,
        )
        # join the sentences back into a full text
        cleaned_text = ". ".join(filtered_sentences)

        return cleaned_text

    df["body"] = df["body"].apply(cleaning_text)

    return df
