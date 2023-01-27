#' @title
#' Importar arquivo da base geral de clientes do c3
#'
#' @description
#' Importa o arquivo c3 para o R ou para o DuckDB.
#'
#' @export
#'
#' @param caminho
#'  String. O caminho do arquivo a ser importado, incluindo a
#'  extensao (.txt).
#' @param import_db
#'  String. "none", "duckdb" ou "postgres".
#' @param usuario
#'  String. Se import_db = "postgres", o usuário deverá ser informado.
#' @param host
#'  String. Se import_db = "postgres", informar o IP do servidor.
#' @param port
#'  Integer. Se import_db = "postgres", informar a porta do servidor.
#' @param senha
#'  String. Se import_db = "postgres", a senha deverá ser informada.
#' @param db
#'  String. Se import_db = "duckdb", o caminho do arquivo do banco de dados.
#'  Se import_db = "postgres", o nome do DB.
#' @param metodo
#'  String. "readr" ou "vroom".
#' @param tabela
#'  String. Nome da tabela a ser criada no DuckDB ou no Postgres.
#'
#' @details
#'  Se for importar apenas para o R, lembre-se de nomear um objeto para
#'  o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#'  criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#'  No caso de importar para o Postgres será necessário um usuário e uma senha e
#'  o servidor deverá estar escutando o IP do usuário.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_c3("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_c3(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = "c3")
#'
#' # para importar para o Postgres
#' import_c3(
#' "\\path\\to\\file.txt",
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' db = "banestes",
#' tabela = "c3_202202")
#' }
#'

import_c3 = function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  tabela,
  metodo = "vroom") {

  # evaluate arg tabela
  if (import_db != "none" && is.null(tabela)) {
    stop("tabela deve ser informada")
  }

  # evaluate arg usuario
  if (import_db == "postgres" && is.null(usuario)) {
    stop("usuário deve ser informado")
  }

  # evaluate arg senha
  if (import_db == "postgres" && is.null(senha)) {
    stop("senha deve ser informada")
  }

  # evaluate arg db
  if (import_db != "none" && is.null(db)) {
    stop("database deve ser informado.
    Se DuckDB, o caminho. Se Postgres, o nome do DB")
  }

  # evaluate arg import_db
  match.arg(
    arg = import_db,
    choices = c("none", "duckdb", "postgres"),
    several.ok = FALSE
  )

  # evaluate arg metodo
   match.arg(
    arg = metodo,
    choices = c("vroom", "readr"),
    several.ok = FALSE
  )


  # conexao com DuckDB

  if (import_db == "duckdb") {

    cli::cli_alert_info("Iniciando conexão com DuckDB")

    con = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )

    cli::cli_alert_success("Conectado com DuckDB")
  }

  # conexao com Postgres

  if (import_db == "postgres") {

    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con = DBI::dbConnect(
      RPostgres::Postgres(),
      host = host,
      port = port,
      user = usuario,
      password = senha,
      dbname = db
    )

    cli::cli_alert_success("Conectado com o banco de dados")
  }

  # especificacoes arquivo
  colunas <- c(
            "agencia", "tp_cli", "cpf_cnpj", "nome", "ano_pn",
            "numero_pn", "status", "valor_pn", "class_pn",
            "class_cliente", "class_garantia", "produto",
            "subproduto", "competencia",
            sapply(1:30, function(x) paste0("exc", x)),
            "proponente",
            unlist(lapply(1:20, function(x) {
                c(
                    paste0("alcada", x),
                    paste0("mat_alc", x),
                    paste0("data", x),
                    paste0("hora", x),
                    paste0("voto", x)
                )
            })),
            unlist(lapply(1:3, function(x) {
                c(
                    paste0("bem", x),
                    paste0("garantia", x),
                    paste0("per_garantia", x)
                )
            })),
            "taxa", "per_taxa", "prazo", "per_prazo", "indice",
            "desc_indice", "per_indice", "origem_pn", "desc_origem_pn",
            "tp_inclusao_pn", "desc_tp_inclusao_pn", "mat_angariador",
            "tp_tx_aceita"
        )
        col_types = readr::cols(
            agencia = readr::col_character(),
            cpf_cnpj = readr::col_character(),
            ano_pn = readr::col_character(),
            numero_pn = readr::col_character(),
            produto = readr::col_character(),
            subproduto = readr::col_character(),
            valor_pn = readr::col_double()
        )

  # importa arquivo para o R via {readr}
  if (metodo == "readr") {

    cli::cli_alert_info("Iniciando importação com readr")

    data <- readr::read_csv(
      caminho,
      col_names  =  colunas,
      col_types  =  col_types
    )
  }

  cli::cli_alert_success("Arquivo importado")

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {

  cli::cli_alert_info("Iniciando importação com vroom")

    data <- vroom::vroom(
      caminho,
      delim  =  ";",
      col_types  =  col_types,
      col_names  =  colunas
    )
  }

  cli::cli_alert_success("Arquivo importado {Sys.time()}")

  # corrigir caracter malformado
  cli::cli_alert_info("Limpando linhas malformadas")
  ini = as.numeric(format(Sys.time(), "%S"))

  data = data |>
    mutate(
      across(
        where(is.character),
        asciify
      )
    )

  fim = as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_success("Linhas removidas em {(fim - ini)} segundos")

  # criar tabela no DuckDB
  if (import_db != "none") {

    # gravar tabela
    ini = as.numeric(format(Sys.time(), "%S"))
    cli::cli_alert_info("Iniciando gravação no DuckDB")

    DBI::dbWriteTable(con, DBI::SQL(tabela), data, append = TRUE)

    fim = as.numeric(format(Sys.time(), "%S"))
    cli::cli_alert_success("Tabela gravada em {(fim - ini) / 60} minutos")

    # desconectando
    duckdb::duckdb_shutdown(duckdb::duckdb())
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
