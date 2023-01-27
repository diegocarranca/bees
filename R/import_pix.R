#' @title
#' Importar arquivo das transações de PIX
#'
#' @description
#' Importa o arquivo PIX para o R, para o DuckDB ou para o Postgres.
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
#'  String. Nome da tabela a ser criada no DuckDB.
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
#' data = import_pix("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_pix(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = "pix")
#'
#' import_pix(
#' "\\path\\to\\file.txt",
#' import_db = "postgres",
#' usuario = "user",
#' senha = "passoword",
#' db = "banestes",
#' tabela = c("pix_recebimento", "pix_pagamento"))
#' }
#'

import_pix = function(
  caminho,
  tipo,
  host = "32.30.10.224",
  port = 5432,
  import_db = "none",
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

  # evaluate args
  match.arg(
    arg = tipo,
    choices = c("recebimento", "pagamento"),
    several.ok = FALSE
  )

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

  # especificacoes arquivo recebimento
    if (tipo == "recebimento") {
        col_types = readr::cols(
        TRN_VL_VALOR = readr::col_character(),
        TRN_CD_AGENCIA_RECEBEDOR = readr::col_character(),
        TRN_NR_CONTA_RECEBEDOR = readr::col_character(),
        TRN_TP_CONTA_RECEBEDOR = readr::col_character(),
        TRN_CD_BANCO_RECEBEDOR = readr::col_character(),
        TRN_CD_BANCO_PAGADOR = readr::col_character(),
        TRN_TP_CHAVE_PAGADOR = readr::col_character(),
        TRN_CD_AGENCIA_PAGADOR = readr::col_character(),
        TRN_NR_CONTA_PAGADOR = readr::col_character(),
        TRN_CD_AGENCIA_PAGADOR = readr::col_character(),
        TRN_TP_CONTA_PAGADOR = readr::col_character(),
        TRN_CD_NSU = readr::col_character(),
        TRN_DH_REGISTRO = readr::col_character(),
        TRN_DT_REFERENCIA = readr::col_character(),
        TRN_ID_PAGADOR = readr::col_character(),
        PAG_VL_VALOR_ORIGINAL = readr::col_double()
    )

    } else {

  # especificando arquivo pagamento
        col_types = readr::cols(
        TRN_VL_VALOR = readr::col_double(),
        TRN_DH_REGISTRO = readr::col_character(),
        TRN_DT_REFERENCIA = readr::col_character(),
        TRN_VL_CHAVE = readr::col_character(),
        TRN_ID_PAGADOR = readr::col_character(),
        TRN_ID_RECEBEDOR = readr::col_character(),
        TRN_DT_EFETIVACAO = readr::col_character(),
        TRN_NR_CONTA_DESTINO = readr::col_character()
        )
    }

  # importa arquivo para o R via {readr}
  if (metodo == "readr") {

    cli::cli_alert_info("Iniciando importação com readr")

    data <- readr::read_csv(
      caminho,
      col_types  =  col_types,
      locale = readr::locale(encoding = "LATIN1")
    )

    cli::cli_alert_success("Arquivo importado")
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {

    cli::cli_alert_info("Iniciando importação com vroom")
    ini <- Sys.time()

    data <- vroom::vroom(
      caminho,
      col_types  =  col_types,
      locale = readr::locale(encoding = "LATIN1")
    )

    fim <- Sys.time()
    cli::cli_alert_success("Arquivo importado em {(fim - ini) / 60} min")
  }

  # corrigir virgulas
  data$TRN_VL_VALOR = as.numeric(
      gsub(
        ",", ".", data$TRN_VL_VALOR
      )
    )

  # corrigir data e hora
  if (tipo == "recebimento") {

  data$TRN_DH_REGISTRO = as.POSIXct(
    data$TRN_DH_REGISTRO, format = "%d/%m/%Y %H:%M:%S",
    tz = Sys.timezone()
  )

  data$TRN_DT_REFERENCIA = as.POSIXct(
    data$TRN_DT_REFERENCIA, format = "%d/%m/%Y %H:%M:%S",
    tz = Sys.timezone()
  )
 }

  if (tipo == "pagamento") {

  data$TRN_DT_REFERENCIA = as.POSIXct(
    data$TRN_DT_REFERENCIA, format = "%d/%m/%Y %H:%M:%S",
    tz = Sys.timezone()
  )


  data$TRN_DH_REGISTRO = as.POSIXct(
    gsub(",", ".",
    data$TRN_DH_REGISTRO), format = "%d/%m/%Y %H:%M:%S",
    tz = Sys.timezone()
  )

  data$TRN_DT_EFETIVACAO = as.POSIXct(
    data$TRN_DT_EFETIVACAO, format = "%d/%m/%Y %H:%M:%S",
    tz = Sys.timezone()
  )
 }

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
  cli::cli_alert_success("Linhas removidas em {(fim - ini) / 60} min")

  # criar tabela no DuckDB
 if (import_db != "none") {

  # gravar tabela

  ini = as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_info("Iniciando gravação para o banco de dados")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data, append = TRUE)

  fim = as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_success("Tabela gravada em {(fim - ini) / 60} minutos")

 # desconectando
  DBI::dbDisconnect(con, shutdown = TRUE)
  remove(con)
  invisible(gc())

  } else {
    data
  }
}
