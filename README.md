# Building Data Pipelines for Modern Data Engineering | End to End Data Engineering Project (Medallion Architecture)

# Tools and Technologies
<img width="1325" height="735" alt="image" src="https://github.com/user-attachments/assets/182c1d81-8787-419b-824a-5552ef9bbf9f" />

We’ll explore how to set up an end-to-end data engineering project using a suite of Azure services, integrating dbt for efficient data transformations. Here’s a detailed breakdown of each component and its role in our project:

Cloud Provider — Azure: Azure will be our foundational cloud platform, providing a robust and scalable environment for all our data operations. Azure’s wide array of services and tools offers a cohesive ecosystem, ensuring seamless integration and management of our data pipeline components.

Database — Azure SQL: We will utilize Azure SQL, the cloud-based version of Microsoft SQL Server, as our primary database system. This choice brings the reliability and familiarity of SQL Server into a flexible, cloud-native environment. For demonstration purposes, we’ll employ the AdventureWorks LT sample database, which provides a rich dataset for various transformation scenarios.

Azure Data Lake: As a central repository for our large-scale data storage and analytics, Azure Data Lake will play a crucial role. It offers a secure, scalable, and cost-effective storage solution, capable of handling massive volumes of structured and unstructured data. This will be instrumental in storing raw data, intermediate results, and final outputs in various formats.

Azure Key Vault: Security and confidentiality are paramount in data engineering projects. Azure Key Vault will be used to securely store and manage sensitive information such as keys, secrets, and certificates. This ensures that our data pipeline maintains high security standards, protecting access to databases, data lakes, and other critical resources.

Azure Databricks: For powerful data processing and analytics, we’ll leverage Azure Databricks. This Apache Spark-based analytics service provides an optimized environment for big data processing and machine learning. It’s instrumental for handling complex data transformation and analysis tasks, offering both performance and ease of use.

Azure Data Factory: As our primary data integration tool, Azure Data Factory will orchestrate and automate the movement and transformation of data. It’s a key component for building data pipelines, allowing us to efficiently manage data flows between various Azure services and external data sources.

dbt (Data Build Tool): dbt will be a key player in our data transformation strategy. It allows for the creation and management of data transformation workflows using familiar SQL syntax. dbt fits seamlessly into our architecture, particularly in the transformation phase, where it can be used to model data, run tests, and generate documentation. This makes it easier for teams, especially those proficient in SQL, to contribute to the data transformation process, ensuring that business logic is consistently applied and documented. Integrating dbt with Azure SQL and Azure Data Lake enhances our ability to manage and transform data efficiently, while maintaining clarity and consistency across the entire pipeline.

# Setting up the stage on Azure Cloud
Getting started with Azure is pretty easy, as they offer free 12 months access to over 55+ services. You can get started by creating a free account. Once that is created, you only need to sign in with your credentials and you will have access to the landing page.

# Setting up the resource groups
Setting up resource groups in Azure is a fundamental step in organizing and managing your Azure resources efficiently. A resource group in Azure is a container that holds related resources for an Azure solution. Once logged in, find the “Resource groups” option in the navigation pane on the left-hand side or list of services. If it’s not visible, use the search bar at the top to search for “Resource groups”, fill in the required details and once the deployment is complete, you can navigate to the resource group and start adding resources like Azure SQL databases, Azure Data Lake storage accounts, etc.
<img width="1902" height="878" alt="image" src="https://github.com/user-attachments/assets/62085022-1ffc-41e5-b4b4-2b835db6ad9e" />

Given our use case and chosen approach, I will set up the necessary containers to support the implementation of the medallion architecture.

<img width="1882" height="811" alt="image" src="https://github.com/user-attachments/assets/839ac630-2156-4975-8365-05b8ff018409" />

We will then create containers for the Bronze, Silver, and Gold layers.
<img width="1905" height="865" alt="image" src="https://github.com/user-attachments/assets/73f10b24-0596-4ccb-bea7-2e89332a4163" />

### Setting Up Azure Data Factory

Azure Data Factory (ADF) enables you to build a data integration solution for creating, scheduling, and orchestrating ETL/ELT workflows. To get started, go to the Azure Portal and search for “Data Factory” using the top search bar or locate it in the navigation menu. Enter the required details, select **Review + Create**, and complete the deployment.

Once deployment is finished, open the Data Factory resource by clicking **Go to resource** or selecting it from your resource list. From the overview page, click on the **Author & Monitor** option to launch Azure Data Factory Studio. This is where you can begin designing and managing your data integration pipelines.

<img width="1648" height="870" alt="image" src="https://github.com/user-attachments/assets/0229844f-5593-475c-a965-094e081aca23" />

### Setting Up Azure Key Vault
Azure Key Vault is used to securely store and manage cryptographic keys, secrets, and certificates required by cloud applications and services. To set it up, go to the Azure Portal and search for “Key Vault” using the search bar or locate it in the navigation menu. Complete the required configuration and deploy the resource.

Once the deployment is complete, open the Key Vault resource. From there, you can begin adding secrets (such as database connection strings), keys (for encryption and decryption), and certificates. You can also configure access policies to control which users or Azure services have permission to access the vault.
<img width="1896" height="853" alt="image" src="https://github.com/user-attachments/assets/807d2d59-ce9a-4ab0-9947-898e49855ec2" />
<img width="1878" height="827" alt="image" src="https://github.com/user-attachments/assets/cd7082b4-786a-47e8-b0b6-bf239b7cce2f" />

### Creating SQL Database and Loading Sample Data
Next, we’ll set up an Azure SQL Database and populate it with the *AdventureWorks sample data* (source). 
In the Azure Portal, search for “SQL Databases” using the search bar or access it through the navigation pane. If you don’t already have an SQL server, you can create one by selecting **Create New** next to the server dropdown in the database configuration section.
<img width="1907" height="822" alt="image" src="https://github.com/user-attachments/assets/1f6bf0f7-322f-4d23-ae76-dd551ab33b04" />
<img width="1873" height="813" alt="image" src="https://github.com/user-attachments/assets/e3b87d70-6d90-4ae2-8a60-7edcde3823a6" />

Under the **Additional Settings** tab, choose the sample database option in the existing data section to load the AdventureWorks LT dataset. After that, proceed with the default steps by clicking **Next** and then **Review + Create** to complete the setup.

### Data Integration with Azure Data Factory
After completing the setup, the next step is to begin building pipelines in Azure Data Factory. You can do this by returning to the ADF Studio page or by searching for “Data Factory” in the top search bar and opening the resource.


### Connecting ADF to Azure SQL Database
The connection between Azure Data Factory (ADF) and the SQL database is established using a linked service. To access this, go to the **Manage** section in the sidebar, then select **Linked Services** under Connections and click **+ New**. Provide the database details and authentication credentials, then test the connection to confirm it has been successfully established.
<img width="1917" height="947" alt="image" src="https://github.com/user-attachments/assets/2ff1f0b2-f27a-4083-9842-330577555696" />

### Connecting ADF to Azure Data Lake Gen2
Similarly, we need to set up a connection to the data lakehouse that hosts our medallion architecture. In this case, we’ll connect to the bronze layer, where raw data is initially ingested before being processed and refined into the silver layer.
<img width="1913" height="938" alt="image" src="https://github.com/user-attachments/assets/e734e998-8c65-4a85-8726-858bebc014d1" />

Dynamic Content allows us to have a parameterised placeholder in our pipeline. We’ll be creating two datasets, SourceTables and SqlTable each of which will help us fetch data from the SQL database dynamically and with parameters.
<img width="1907" height="902" alt="image" src="https://github.com/user-attachments/assets/9b487d08-bbbf-40ba-bec0-25fdba58f211" />

SourceTables: Create a new dataset using the AzureSqlDatabase1 linked service defined earlier to dynamically retrieve the list of tables from the database’s information schema. This dataset is created without specifying a table name.

SqlTable: Similarly, create the SqlTable dataset using the same AzureSqlDatabase1 linked service. This dataset is also defined without a fixed table name, but it will include parameters to allow dynamic table selection.
<img width="1917" height="940" alt="image" src="https://github.com/user-attachments/assets/4707a457-a465-4f93-b570-28586ae2a1c9" />

Returning to the pipeline, we need to add a Lookup activity to the canvas. Within this activity, we’ll configure it to use the TablesQuery dataset. In the Settings tab, select the Query option and provide the required query.
<img width="1915" height="902" alt="image" src="https://github.com/user-attachments/assets/33326d12-d544-4e77-9210-18f2ac48a93c" />

### Iterating Through the Lookup Results
Once the query executes, the TablesQuery dataset is automatically populated with an array containing fields such as table_catalog, table_schema, table_name, and table_type. To process each of these records, we use a ForEach activity.

### ForEach Activity
The ForEach activity allows us to access and iterate over individual records within the array returned by the Lookup. To set this up, drag and drop the ForEach activity onto the pipeline and configure its Items property using dynamic content.
<img width="1917" height="906" alt="image" src="https://github.com/user-attachments/assets/583433b1-f1a4-40a1-a3e1-46bdffe914a8" />

<img width="1917" height="910" alt="image" src="https://github.com/user-attachments/assets/2c13bdfb-ae7f-4beb-a07e-dc2ebd1dea52" />
### Loading Table Data into the Bronze Layer
After configuring the ForEach activity to use @activity('Fetch All Tables').output.value, the next step is to define the actions to be executed for each item in the array.

To achieve this, add a Copy Data activity from the Move & Transform section into the ForEach pipeline. Configure it to use the second dataset (SqlTable), which supports parameterization. Below is how the source is set up.

<img width="1917" height="898" alt="image" src="https://github.com/user-attachments/assets/5daff713-f648-4c4f-be50-7722f904b36c" />

For the sink, select the Parquet file format and create a Parquet output dataset in Azure Data Lake Storage Gen2. This will use the linked service that was configured earlier. Set the file path to bronze, and leave the directory and file name empty, as these will be dynamically populated using parameters.
<img width="1912" height="907" alt="image" src="https://github.com/user-attachments/assets/74dfb7f0-b6fe-4fcd-bd14-de9b0bd5eb14" />
<img width="1912" height="905" alt="image" src="https://github.com/user-attachments/assets/48474afd-d243-428b-93e0-717ad984869c" />
In the pipeline, configure the sink by passing the required parameters (FolderName and FileName), with the file path set to bronze.
<img width="1913" height="902" alt="image" src="https://github.com/user-attachments/assets/91113fd7-fca9-4276-b4f9-64d10f938b0e" />

The final step in completing the Copy Data activity is to select ParquetFileOutput as the sink from the dropdown. Below are the expressions used for the FolderName and FileName parameters:

Folder: @formatDateTime(utcnow(), 'yyyyMMdd')

File: @concat(item().table_schema, '.', item().table_name, '.parquet')
<img width="1913" height="897" alt="image" src="https://github.com/user-attachments/assets/0b4a146b-c357-4d85-88a5-213c21432bf8" />
If you trigger (debug) this pipeline, you will be able to see the data dumped into the bronze layer correctly.
<img width="1913" height="897" alt="image" src="https://github.com/user-attachments/assets/f2c7a4c5-96eb-4f35-827c-305981c0daa1" />

<img width="1918" height="912" alt="image" src="https://github.com/user-attachments/assets/abd4be8b-0475-4d3d-a71a-a30754dd20a7" />
In your bronze container you should be seeing parquet files:
<img width="1915" height="915" alt="image" src="https://github.com/user-attachments/assets/b99d9046-522b-4d33-8ccb-711963732b61" />

The final step in the pipeline is to add the Databricks notebook activity. We’ll return to this after completing the Azure Databricks setup.


