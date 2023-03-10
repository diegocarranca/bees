% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/import_gco.R
\name{import_gco}
\alias{import_gco}
\title{Importar arquivo de contratos de credito}
\usage{
import_gco(
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

\item{db}{String. Se duckdb = TRUE, o caminho do arquivo do banco de dados.}

\item{tabela}{String. Nome da tabela a ser criada no DuckDB.}

\item{duckdb}{Logical.
Se TRUE, entao o arquivo sera importado para o DuckDB.}
}
\description{
Importa o arquivo GCOA53 para o R ou para o DuckDB.
}
\details{
Se for importar apenas para o R, lembre-se de nomear um objeto para
o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
criar um objeto. Note que a tabela sera sobrescrita se ja existir.
}
\examples{
\dontrun{
# para importar para o R:
data = import_gco("\\\\path\\\\to\\\\file.txt")

# para importar para o DuckDB:
import_bg(
 "\\\\path\\\\to\\\\file.txt",
 duckdb = TRUE,
 db = "\\\\path\\\\to\\\\duck.db"),
 tabela = "bg_pf"
}

}
