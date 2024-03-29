from transformers import BartTokenizer, BartForConditionalGeneration

def generate_summary(text):
    tokenizer = BartTokenizer.from_pretrained("facebook/bart-large-cnn")
    model = BartForConditionalGeneration.from_pretrained("facebook/bart-large-cnn")

    inputs = tokenizer([text], max_length=1024, return_tensors="pt", truncation=True)
    summary_ids = model.generate(inputs["input_ids"], num_beams=4, min_length=30, max_length=150, early_stopping=True)

    summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True)
    return summary

# Example usage:
search_result_text = """"


"""
summary = generate_summary(search_result_text)
print(summary)
