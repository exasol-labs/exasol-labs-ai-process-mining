
from dataclasses import dataclass



@dataclass
class ResultText:
  result_text: str

  def __str__(self):
        return self.result_text

  

def wrap_text(text: str) -> str:

  try:
    return  text
  except error as e:
    print(e)
    