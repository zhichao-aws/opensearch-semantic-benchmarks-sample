import json
import tokenizers

class TokenizerWrapper:
    def __init__(self, tokenizer_path = "tokenizer.json", idf_path = "idf.json"):
        # we use low-level tokenizer API instead of transformers, and we can get words and ids at one time
        self.tokenizer = tokenizers.Tokenizer.from_file(tokenizer_path)
        with open(idf_path, "r") as f:
            self.idf = json.load(f)
        self.unk_token = self.tokenizer.model.unk_token

        # use array to store idf values and token id as index, faster than dict
        vocab = self.tokenizer.get_vocab()
        id_to_idf = [1.0 for _ in range(len(vocab))]
        for word, id in vocab.items():
            id_to_idf[id] = self.idf.get(word, 1.0)
        self.id_to_idf = id_to_idf

    def encode(self, text):
        encoding_res = self.tokenizer.encode(text, add_special_tokens=False)
        idf_values = [self.id_to_idf[id] for id in encoding_res.ids]
        res = dict(zip(encoding_res.tokens, idf_values))
        if self.unk_token in res:
            res.pop(self.unk_token)
        return res