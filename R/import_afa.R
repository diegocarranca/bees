#' @title
#' Importar arquivo da base geral de clientes do AFA
#'
#' @description
#' Importa o arquivo AFA para o R ou para o DuckDB.
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
#' Se for importar apenas para o R, lembre-se de nomear um objeto para
#' o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#' criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' No caso de importar para o Postgres será necessário um usuário e uma senha e
#' o servidor deverá estar escutando o IP do usuário.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_afa("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_afa(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = "afa")
#'
#' # para importar para o Postgres:
#' import_afa(
#' "\\path\\to\\file.txt"),
#'  import_db = "postgres",
#'  usuario = "user",
#'  senha = "password",
#'  db = "banestes",
#'  tabela = "afa")
#' }
#'

import_afa = function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  metodo = "vroom",
  tabela) {

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

  # evaluate args metodo
  match.arg(
    arg = metodo,
    choices = c("vroom", "readr"),
    several.ok = FALSE
  )

  # evaluate arg import_db
  match.arg(
    arg = import_db,
    choices = c("none", "duckdb", "postgres"),
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

  # especificacoes arquivo
    colunas = c(
      "fundo", "conta", "sequencia", "saldo_liqui",
     "resgate_automatico", "dt_movimento", "tp_cli",
     "cpfcnpj", "ag_banco", "branco"
    )

    larguras = c(
      2, 10, 4, 14, 1, 8, 1, 14, 4, 2
    )

    col_types = readr::cols(
      saldo_liqui = readr::col_double(),
      dt_movimento = readr::col_date("%Y%m%d")
    )

  # importar arquivo para o R via {readr}
  if (metodo == "readr") {

    cli::cli_alert_info("Iniciando importação com readr")

    data = readr::read_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 1
    )

    cli::cli_alert_success("Arquivo importado")
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {

    cli::cli_alert_info("Iniciando importação com vroom")
    ini = as.numeric(format(Sys.time(), "%S"))

    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 1
    )

    fim = as.numeric(format(Sys.time(), "%S"))
    cli::cli_alert_success("Arquivo importado {(fim - ini)} segundos")
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
  cli::cli_alert_success("Linhas removidas em {(fim - ini)} segundos")

  # criar tabela no DuckDB
  if (import_db != "none") {

  # dropar tabela
    DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", tabela))
    cli::cli_alert_success("Tabela dropada")

  # gravar tabela
    ini = as.numeric(format(Sys.time(), "%S"))
    cli::cli_alert_info("Iniciando gravação para o banco de dados")

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    fim = as.numeric(format(Sys.time(), "%S"))
    cli::cli_alert_success("Tabela gravada em {(fim - ini) / 60} minutos")

    # gravar índices
    cli::cli_alert_info("Iniciando gravação de índices")

    if (import_db == "duckdb") {
      DBI::dbExecute(con, paste0("
      ALTER TABLE ", tabela, "
      ADD INDEX idx_afa_cpfcnpj (cpfcnpj(14)),
      ADD INDEX idx_afa_conta (conta(10)),
      ADD INDEX idx_afa_dt_movimento (dt_movimento(8));
      "))
    }

    if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_afa_conta ON ", tabela, " (conta);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_afa_cpfcnpj ON ", tabela, " (cpfcnpj);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_afa_dt_movimento ON ", tabela, " (dt_movimento);"))
    }

    cli::cli_alert_success("Índices criados")

  # desconectando
    DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
