import os
import random

import pandas as pd


def is_programming_lang(lang: str):
    non_prog_list = [
        "xml",
        "xmlt",
        "yml",
        "yaml",
        "json",
        "toml",
        "ini",
        "docker",
        "vagrant",
        "markdown",
        "asciidoc",
        "css",
        "latex",
        "rmarkdown",
        "csv",
        "sql",
        "sparql",
        "promql",
        "kvlang",
        "kv",
        "reStructuredText",
        "vue",
    ]
    ret = True
    for l in non_prog_list:
        if l in lang.lower():
            return False
    return ret


def guess_languages_from_special_files(df):
    special_files_mapping = {
        "setup.py": ["Python"],
        "requirements.txt": ["Python"],
        "build.gradle": ["Java", "Groovy", "Kotlin", "Scala"],
        "package.json": ["JavaScript", "TypeScript"],
        "go.mod": ["Go"],
        "Cargo.toml": ["Rust"],
        "composer.json": ["Ruby"],
        "cabal.json": ["Haskell"],
        "Rakefile": ["Ruby"],
        "GemFile": ["Ruby"],
        ".sln": ["C#"],
        ".csproj": ["C#"],
        "build.sbt": ["Scala"],
        "pom.xml": ["Java"],
    }

    def is_special_file(path):
        try:
            special_files_mapping[os.path.basename(path)]
            return True
        except Exception as e:
            return False

    all_files = df["title"].to_list()
    special_files = list(filter(is_special_file, all_files))

    pro = list(map(lambda x: special_files_mapping[os.path.basename(x)], special_files))

    probable_languages = set([item for y in pro for item in y])
    return probable_languages


def segregate_ansible_openshift_yaml(table):
    #  analyses the dataframe for yamls to check if they are ansible yamls or openshfit yamls
    pass


def get_dominant_language(table):
    df = table.to_pandas()
    lan = df["language"].value_counts()

    languages_guessed_from_special_files = guess_languages_from_special_files(df)
    top_languages_info = lan.nlargest(3)
    top_languages = top_languages_info.index.values
    top_programming_languages = list(filter(is_programming_lang, top_languages))
    most_occuring = lan.idxmax()
    if len(top_languages) > 1:
        second_most_occuring = top_languages[1]
    else:
        second_most_occuring = most_occuring
    print(f"Most promiment languages:  [{most_occuring} ,{second_most_occuring}]")

    # a programming language occurs is most of the rows
    if is_programming_lang(most_occuring):
        if most_occuring == second_most_occuring:
            # case 1: uneqully popular, choose most popular
            return most_occuring
        # two languages are equally prominent
        if top_languages_info[most_occuring] == top_languages_info[second_most_occuring]:
            # most popular language is a programming language,
            # multiple languages
            # case 2: equally popular, check probable language, choose probable language
            print(f"{most_occuring} and {second_most_occuring} are equally prominent in the repo")
            print("Let us try to detect using special files")
            lang_set = set([most_occuring, second_most_occuring])
            chosen_lang_set = lang_set.intersection(languages_guessed_from_special_files)
            if len(chosen_lang_set) > 0:
                print("langs:", lang_set)
                print("guessed:", languages_guessed_from_special_files)
                print("chosen:", chosen_lang_set)
                print("choosing language: ", chosen_lang_set)
                # when there is a language common in top_programming_languages and
                # languages_guessed_from_special_files, the order in which it appears in
                # top_programming_languages is used for choosing the language
                chosen_lang = chosen_lang_set.pop()
                return chosen_lang
            # TODO: if there is an preference list for languages it can be used here.
            # since we are unable to decide which language to choose, randomly choose one
            chosen_language = random.choice([most_occuring, second_most_occuring])
            print(
                f"Not much info is available to decide between {most_occuring} and {second_most_occuring}. So choosing {chosen_language}"
            )
            return chosen_language

    # Is the most popular language not a programming language
    if not is_programming_lang(most_occuring):
        # is there a programming language in top 3
        if len(top_programming_languages) > 0:
            # check intersection of guessed and top
            top_lang_set = set(top_programming_languages)
            chosen_lang_set = top_lang_set.intersection(languages_guessed_from_special_files)
            if len(chosen_lang_set) > 0:
                print("top:", top_programming_languages)
                print("guessed:", languages_guessed_from_special_files)
                print("chosen:", chosen_lang_set)
                print("choosing language: ", chosen_lang_set)
                # when there is a language common in top_programming_languages and
                # languages_guessed_from_special_files, the order in which it appears in
                # top_programming_languages is used for choosing the language
                chosen_lang = chosen_lang_set.pop()
                return chosen_lang
            # we were not able to guess this language so, choose the most frequent
            # programming language
            if "yaml" in most_occuring.lower():
                # TODO: list cases when whe have shell and yaml together and shell is more than YAML
                print(f"YAML and {top_programming_languages[0]} found. Check the repo for detailed analysis")
            return top_programming_languages[0]

    print(f"returning from the end of function. chosen language: {most_occuring}")
    return most_occuring


# Modified get_dominant_langauge for preparing data with repo-packing
def get_dominant_language_repo_packing(table):
    df = table.to_pandas()
    lan = df["language"].value_counts()

    top_languages_info = lan.nlargest(3)
    top_languages = top_languages_info.index.values
    most_occuring = lan.idxmax()

    if len(top_languages) == 1:
        # case 1: single most popular language, choose most popular
        print(f"Single most promiment langauge:  {most_occuring}")
        return most_occuring

    second_most_occuring = top_languages[1]
    print(f"Most promiment languages:  [{most_occuring} ,{second_most_occuring}]")

    # two languages are equally prominent
    if top_languages_info[most_occuring] == top_languages_info[second_most_occuring]:
        # multiple languages
        # case 2: equally popular, check probable language, choose probable language
        print(f"{most_occuring} and {second_most_occuring} are equally prominent in the repo")
        # detect using special files
        languages_guessed_from_special_files = guess_languages_from_special_files(df)
        lang_set = set([most_occuring, second_most_occuring])
        chosen_lang_set = lang_set.intersection(languages_guessed_from_special_files)
        if len(chosen_lang_set) > 0:
            print(f"guessed:{languages_guessed_from_special_files}, chosen set:{chosen_lang_set}")
            # when there is a language common in top_languages and
            # languages_guessed_from_special_files, the order in which it appears in
            # top_languages is used for choosing the language
            chosen_lang = chosen_lang_set.pop()
            return chosen_lang

        # since we are unable to decide which language to choose, randomly choose one
        chosen_language = random.choice([most_occuring, second_most_occuring])
        print(
            f"Not much info is available to decide between {most_occuring} and {second_most_occuring}. So choosing {chosen_language}"
        )
        return chosen_language

    print(f"returning from the end of function. chosen language: {most_occuring}")
    return most_occuring
