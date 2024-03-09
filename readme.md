<h1>Into to Data Eng - Pacmann Assignment</h1>

<p>This project is part of assesment in Pacmann Course</p>

<h2>Requirement Gathering and Solution</h2>

<h3>Problem</h3>
<ul>
  <li>Sales team have have the amazon sales data but the current data is dirty</li>
  <li>Analyst team want to know the condition of market for their marketing plan and they expect the data to be formatted as they wish </li>
  <li>A company plans to expand their store to Indonesia and they need to know list of district and provinces in Indonesia</li>
  <li>The data is from varies source and the comapny want to store the data within postgresql</li>
</ul>

<h3>Data Source</h3>
<ul>
  <li>Sales data: https://hub.docker.com/r/shandytp/amazon-sales-data-docker-db</li>
  <li>Marketing Data: https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view?usp=sharing</li>
  <li>Indonesia City: https://github.com/emsifa/api-wilayah-indonesia</li>
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
      <li>Save the output of indonesia are data to indo_area_data table on posgresql</li>
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
<h3>Output</h3>
<ul>
  <li>Table sales data</li>
  <li>Table marketing data</li>
  <li>Table indo region data</li>
</ul>
