from fasttext import load_model


class FastTextLangPredictor:
    def __init__(self, model_path="./ft_weights/lid.176.bin"):
        self.model = load_model(model_path)

    def predict(self, text, k=1):
        label, prob = self.model.predict(text, k)

        if isinstance(text, str):
            # 単一のテキストの場合
            return list(zip([l.replace("__label__", "") for l in label], prob))
        elif isinstance(text, list):
            # テキストのリストの場合
            results = []
            for labels, probs in zip(label, prob):
                # 各予測結果の最初の要素のみを取得
                l = labels[0].replace("__label__", "")
                p = probs[0]
                results.append((l, p))
            return results
        else:
            raise ValueError("Input 'text' must be either a string or a list of strings.")