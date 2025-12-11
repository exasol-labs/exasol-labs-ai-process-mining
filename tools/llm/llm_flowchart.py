from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field


class DisplayResult(BaseModel):
    display_result: str = Field(
        description="The result set converted into a nice and shiny table in MARKDOWN syntax."
    )

system_prompt = """
You are a helpful assistant that analyzes data structures for a flowchart application.
The data contains transitions between process steps. 
Your task: analyze the flowchart, detect outliers, and create a table for each node
with the number of passengers visiting. 

The number shows the number of passengers which travels between two nodes.

A process step can occur multiple times in the flowchart, always  sum up of all equal steps.
Separate the follwoing steps: Dining Area, Lounge, and Duty Free.

Use the following title: Airport Passenger Analysis for Terminal-1

Format your result im Markdown syntax.
"""

def llm_flowchart_analysis(llm_server:str, llm_api_token: str, flowchart: str, temperature: float) -> str:

    flowchart_data = f"""
Please analyze the following flowchart table:

{flowchart}
    
"""
    
    result_AI_analysis = None
    
    llm = ChatOpenAI(model_name="qwen/qwen3-coder-30b",
                             temperature=0.25,
                             openai_api_base=f"{llm_server}",
                             openai_api_key=f"{llm_api_token}",
                             )
                             
    
     
    
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            ("human", "{flowchart_data}"),
        ]
    )

    messages = prompt.format_messages(flowchart_data=flowchart_data) 
  
    #structured_llm = llm.with_structured_output(DisplayResult)
    #process = t2s_prompt | structured_llm
    
    #result_AI_analysis = process.invoke({"question": question})
    result_AI_analysis = llm.invoke(messages)
    return result_AI_analysis.content