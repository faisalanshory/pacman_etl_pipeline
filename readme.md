<h1>Into to Data Eng - Pacmann Assignment</h1>

<p>This project is part of assesment in Pacmann Course</p>

<h2>Requirement Gathering and Solution</h2>

<h3>Problem</h3>
<ul>
  <li>The sales team possesses Amazon sales data, but it is currently unclean or messy.</li>
  <li>The analyst team seeks insights into market conditions to inform their marketing strategies, desiring the data to adhere to specific formatting requirements.</li>
  <li>A collaboration between a company and an NGO aims to develop an NLP model for disaster research in Indonesia, necessitating the collection of relevant Indonesian journal articles on disasters.</li>
  <li>The company intends to centralize data storage within a PostgreSQL database, sourced from multiple diverse origins.</li>
</ul>

<h3>Data Source</h3>
<ul>
  <li>Sales data: https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db</li>
  <li>Marketing Data: https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view?usp=sharing</li>
  <li>Disaster Journal: https://garuda.kemdikbud.go.id/journal/view/1242</li>
</ul>

<h3>Goal</h3>
<p>Collect data from multiple sources and format it in table and store it in the postgreqsl</p>

<h2> Data Engineer Solution</h2>
<ul>
  <li>Make an ETL Pipeline</li>
  <ol>
    <li>Extract</li>
    <ul>
      <li>Collect the data</li>
      <li>Understanding the data</li>
      <li>Check data types and values</li>
    </ul>
    <li>Transform</li>
    <ul>
      <li>Filtering the usable column</li>
      <li>Formatting the column name</li>
      <li>Formatting the table</li>
      <li>Cleaning the data value</li>
    </ul>
    <li>Load</li>
    <ul>
      <li>Save the output of sales data to sales_data table on posgresql</li>
      <li>Save the output of marketing data to marketing_data table on posgresql</li>
      <li>Save the output of indonesia disaster journal to journal_data table on posgresql</li>
    </ul>
    <li>Automate the task</li>
    <ul>
      <li>Make a scheduler task with cronjob</li>
    </ul>
  </ol>
  <li>Tools: Python, Pandas, Luigi, VS code, and Postgresql</li>
  <li>ETL Design Pipeline</li>
  <p>Test</p>
</ul>
<h3>How To Use It</h3>
![Image Output]([https://github.com/your_username/your_repository/raw/main/image.png](https://github.com/faisalanshory/pacman_etl_pipeline/blob/main/img/pipeline.jpg))
<h3>Output</h3>
<ul>
  <li>Table sales data</li>
  <li>Table marketing data</li>
  <li>Table indo region data</li>
</ul>
