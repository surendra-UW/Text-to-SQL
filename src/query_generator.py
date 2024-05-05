from openai import OpenAI

client = OpenAI(
  base_url = "https://integrate.api.nvidia.com/v1",
  api_key = "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"
)

def generate_query(prompt):
    completion = client.chat.completions.create(
      model="mistralai/mixtral-8x22b-instruct-v0.1",
      messages=[{"role":"user","content": prompt}],
      temperature=0.5,
      top_p=1,
      max_tokens=1024,
      stream=False
    )

    return completion.choices[0].message.content