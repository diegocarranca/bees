% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/import_x4.R
\name{import_x4}
\alias{import_x4}
\title{Importar arquivo de folha de pagamento}
\usage{
import_x4(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  tabela,
  metodo = "vroom"
)
}
\arguments{
\item{caminho}{String. O caminho do arquivo a ser importado, incluindo a
extensao (.txt).}

\item{import_db}{String. "none", "duckdb" ou "postgres".}

\item{host}{String. Se import_db = "postgres", informar o IP do servidor.}

\item{port}{Integer. Se import_db = "postgres", informar a porta do servidor.}

\item{usuario}{String. Se import_db = "postgres", o usuário deverá ser informado.}

\item{senha}{String. Se import_db = "postgres", a senha deverá ser informada.}

\item{db}{String. Se import_db = "duckdb", o caminho do arquivo do banco de dados.
Se import_db = "postgres", o nome do DB.}

\item{tabela}{String. Nome da tabela a ser criada no DuckDB.}

\item{metodo}{String. "readr" ou "vroom".}
}
\description{
Importa o arquivo X4 do sistema DPI para o R, para o DuckDB ou para o Postgres.
}
\details{
Se for importar apenas para o R, lembre-se de nomear um objeto para
o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
criar um objeto. Note que a tabela sera sobrescrita se ja existir.

No caso de importar para o Postgres será necessário um usuário e uma senha e
o servidor deverá estar escutando o IP do usuário.
}
\examples{
\dontrun{
# para importar para o R:
data = import_x4("\\\\path\\\\to\\\\file.txt")

# para importar para o DuckDB:
import_x4(
 "\\\\path\\\\to\\\\file.txt",
 import_db = "duckdb",
 db = "\\\\path\\\\to\\\\duck.db",
 tabela = "x4_202202")

# para importar para o Postgres:
import_x4(
"\\\\path\\\\to\\\\file.txt"),
import_db = "postgres",
usuario = "user",
senha = "password",
db = "banestes"
tabela = "x4_202202")
}

}
