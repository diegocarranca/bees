#' @title
#' Importar arquivo de transações do cartão Banescard
#'
#' @description
#' Importar o arquivo Banescard para o R, para o DuckDB ou para o Postgres.
#'
#' @export
#'
#' @param caminho
#' String. O caminho do arquivo a ser importado, incluindo a
#' extensao (.csv).
#' @param import_db
#' String. "none", "duckdb" ou "postgres".
#' @param usuario
#' String. Se import_db = "postgres", o usuário deverá ser informado.
#' @param host
#' String. Se import_db = "postgres", informar o IP do banco de dados.
#' @param port
#' Int. Se import_db = "postgres", informar a porta.
#' @param senha
#' String. Se import_db = "postgres", a senha deverá ser informada.
#' @param db
#' String. Se import_db = "duckdb", o caminho do arquivo do banco de dados.
#' Se import_db = "postgres", o nome do DB.
#' @param metodo
#' String. "readr" ou "vroom".
#' @param tabela
#' String. Nome da tabela a ser criada no DuckDB.
#'
#' @details
#' Se for importar apenas para o R, lembre-se de nomear um objeto para
#' o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#' criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' No caso de importar par ao Postgres será necessário um usuário e uma senha.
#' Além disso, o IP também deve ser autorizado.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_banescard("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_banescard(
#' "\\path\\to\\file.txt",
#' import_db = "duckdb",
#' db = "\\path\\to\\duck.db",
#' tabela = "banescard")
#'
#' # para importar para o Postgres:
#' import_banescard(
#' "\\path\\to\\file.txt",
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' tabela = "banescard")
#'}
#'

import_banescard = function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  tabela = "banescard",
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

    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )

    cli::cli_alert_success("Conectado com o banco de dados")
  }

  # conexao com postgres

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

  # especificações do arquivo

   col_types = readr::cols(
            SUPERINTENDENCIA = readr::col_character(),
            DATA_TRANSACAO = readr::col_date("%d/%m/%Y"),
            VALOR_TRANSACAO = readr::col_character(),
            TIPO_TRANSACAO = readr::col_character(),
            N_PLASTICO = readr::col_character(),
            TITULARIDADE = readr::col_character(),
            NOME_CLIENTE = readr::col_character(),
            CPF = readr::col_character(),
            PRODUTO = readr::col_character(),
            TIPO_CONTA = readr::col_character(),
            CONTRATO = readr::col_character(),
            ST_CONTRATO_CREDITO = readr::col_character(),
            ST_CONTA_CARTAO = readr::col_character(),
            LIMITE = readr::col_character()
        )

  # importa arquivo para o R via {readr}

  if (metodo == "readr") {

    cli::cli_alert_info("Iniciando importação com readr")

    data <- readr::read_csv(
      caminho,
      col_types  =  col_types,
      locale = readr::locale(encoding = "LATIN1")
    )
  }

  # importar arquivo para o R via {vroom}

  if (metodo == "vroom") {

    cli::cli_alert_info("Iniciando importação com vroom")
    ini = as.numeric(format(Sys.time(), "%S"))

    data = vroom::vroom(
      caminho,
      col_types  =  col_types,
      locale = readr::locale(encoding = "LATIN1")
    )

   fim = as.numeric(format(Sys.time(), "%S"))
   cli::cli_alert_success("Importação concluída {(fim - ini)} segundos")
  }

  # corrigir vírgulas
  data$VALOR_TRANSACAO = as.numeric(
      gsub(
        ",", ".", data$VALOR_TRANSACAO
      )
    )

  # corrigir caracter malformado

  cli::cli_alert_info("Limpando linhas malformadas {Sys.time()}")

  data = data |>
    mutate(
      across(
        where(is.character),
        asciify
      )
    )

   fim = as.numeric(format(Sys.time(), "%S"))
   cli::cli_alert_success("Linhas removidas {Sys.time()}")

  # criar tabela no DuckDB
   if (import_db != "none") {

  # gravar tabela
   ini = as.numeric(format(Sys.time(), "%S"))
   cli::cli_alert_info("Iniciando gravação para o banco de dados")

   DBI::dbWriteTable(con, DBI::SQL(tabela), data, append = TRUE)

   fim = as.numeric(format(Sys.time(), "%S"))
   cli::cli_alert_success("Gravação concluída em {(fim - ini) / 60} minutos")

  # desconectando
   DBI::dbDisconnect(con, shutdown = TRUE)
   remove(con)
   invisible(gc())

  } else {
    data
  }
}
