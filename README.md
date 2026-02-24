# Process Mining with Exasol.

## Disclaimer

The accuracy of Large Language Models (LLM) depend on the training set used, and on further  
methods like Retrieval Augmented Generation (RAG). Large Language Models can make mistakes and  
may produce faulty or wrong results!  

Check each result produced by the Process Mining demonstrator 


Nor Exasol as a company, nor the author(s) of the Process Minin g demonstartor can be held liable for  
damage to any possible kind by using this software. Moreover, the Safe Harbour Statement at the  
end of this README shall remain valid.

__Do NOT use the Process-Mining demonstrator if you do not agree to these conditions!__


## Important information

This demosntartor is highly experimental and only in a pre-alpha state. It may lead to inconsitencies. One known bug is that the GUI always jumps baxck to the first tab when changing filter conditions and/or other menus. You need to navigate back to the section manually. The code is not fully structured and many code
segemnts need to be relocated in functions, or modules.

## Installation

### Sample Data Set

In the repository is a demo data set for so-called Airport Passenger Flow Analysis. It contains data for two  
months of passengers departing from an airport.

Uncompress and execute the SQL file from the Sample_Data direcory. It creates the required data structures. 
and the loads  approx. 2.5 Mio rows. There may be some data issues visisble as this synthetic data with no real linkage to a real airport. 

### Install perequisites

Install the following Python packages. 


    "altair>=6.0.0",
    "ibis-framework[exasol]>=10.8.0",
    "ipython>=9.8.0",
    "langchain-openai>=0.3.35",
    "marimo>=0.19.2",
    "msgspec>=0.19.0",
    "polars>=1.34.0",
    "pydantic>=2.12.2",
    "pyexasol>=1.2.0",
    "python-dotenv>=1.1.1",
    "sqlalchemy-exasol>=5.2.0",


It is recommended to install in a fresh Python virtual environemnt. Use
the command based on your fasvorite tool. For Python3 you can use:


    python3 -m venv .venv

Create a ".env" file in your working directory for the Process Mining demonstrator and add/adapts the follwing  
environment variables to it:

    KEA_PROCESS_INSIGHTS_EXA_DB_SERVER="<Your Exasol Database Server hostname>"
    KEA_PROCESS_INSIGHTS_EXA_DB_PORT=8563
    KEA_PROCESS_INSIGHTS_EXA_DB_FINGERPRINT="<Fingerprint of Exasol Database when using self-signed certificates>>"
    KEA_PROCESS_INSIGHTS_EXA_DB_USER="<Your Exasol Database Username>"
    KEA_PROCESS_INSIGHTS_EXA_DB_PASSWORD="Your Exasol Database Password"
    KEA_PROCESS_INSIGHTS_EXA_DB_SCHEMA="<Your Database Schema>"
    KEA_PROCESS_INSIGHTS_LLM_SERVER_URL="Your LLM Seerver, e.g. http://localhost:1234/v1"
    KEA_PROCESS_INSIGHTS_LLM_API_TOKEN="<LLM-Server API Token>"
    KEA_PROCESS_INSIGHTS_LLM_MODEL="<The LLM I am using: >wen/qwen3-coder-30b>"

### Install application

Start the application with:

    marimo edit process_insights.py

or

    marimo run process_insights.py

It will take a moment until the application is available for usage.

### Usage

Please see the following articles for further informartion:

    https://www.exasol.com/blog/process-mining-with-exasol/



## You do not have access to an Exasol Database?

No worries, we have a solution for you:


    https://www.exasol.com/de/personal/

or

    https://www.exasol.com/free-signup-community-edition/


Both variants are fully sufficient to server ther Process Mining demonstrator.


## License

This project is licensed under the MIT License - see the LICENSE file for details.

### Safe Harbor Statement: Exasol MCP Server & AI Solutions

Exasol’s AI solutions (including MCP Server) are designed to enable intelligent,
autonomous, and highly performant access to data through AI and LLM-powered agents.
While these technologies unlock powerful new capabilities, they also introduce
potentially significant risks.

By granting AI agents access to your database, you acknowledge that the behavior of
large language models (LLMs) and autonomous agents cannot be fully predicted or
controlled. These systems may exhibit unintended or unsafe behavior—including but not
limited to hallucinations, susceptibility to adversarial prompts, and the execution of
unforeseen actions. Such behavior may result in data leakage, unauthorized data
generation, or even data modification or deletion.

Exasol provides the tools to build AI-native workflows; however, you, as the implementer
and system owner, assume full responsibility for managing these solutions within your
environment. This includes establishing appropriate governance, authorization controls,
sandboxing mechanisms, and operational guardrails to mitigate risks to your organization,
your customers, and their data.