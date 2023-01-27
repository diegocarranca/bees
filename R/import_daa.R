#' @title
#' Importar arquivo das informações de poupança
#'
#' @description
#' Importa o arquivo DAA para o R, para o DuckDB ou para o Postgres.
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
#' data = import_daa("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_daa(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db"),
#'  tabela = "daa"
#' }
#'
#' # para importar para o Postgres:
#' import_daa(
#' "\\path\\to\\file.txt"),
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' tabela = "daa")
#'

import_daa = function(
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


  # especificacoes arquivo
  colunas = c(
     "conta", "tipo", "cpfcnpj", "ligada", "nome", "agencia",
     "saldo_positivo", "saldo_negativo", "saldo_provisao",
     "conta_provisao", "conta_contabil", "conta_cosif",
     "dt_abertura", "dt_ult_mov", "tp_conta", "saldo_velho",
     "saldo_novo", "saldo_neg_velho", "saldo_neg_novo",
     "saldo_prov_velho", "conta_prov_novo",
     "matricula1", "matricula2", "desconhecido", "CID"
    )


  larguras = c(
        20, 1, 14, 3, 30, 30, 14, 14, 16, 5, 5, 8, 8, 8, 2, 14, 14, 14, 14,
        16, 16, 9, 9, 12, 4
    )

  col_types = readr::cols(
        tipo = readr::col_character(),
        cpfcnpj = readr::col_character(),
        tp_conta = readr::col_character(),
        conta_cosif = readr::col_character(),
        conta_provisao = readr::col_character(),
        saldo_positivo = readr::col_double(),
        saldo_negativo = readr::col_double(),
        saldo_provisao = readr::col_double(),
        saldo_velho = readr::col_double(),
        saldo_novo = readr::col_double(),
        saldo_neg_velho = readr::col_double(),
        saldo_neg_novo = readr::col_double(),
        saldo_prov_velho = readr::col_double(),
        dt_abertura = readr::col_date("%Y%m%d"),
        dt_ult_mov = readr::col_date("%Y%m%d")
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
  cli::cli_alert_success("Linhas removidas em {(fim - ini)} segundos")
  }

  # corrigir caracter malformado
  cli::cli_alert_info("Limpando linhas malformadas {Sys.time()}")

  data = data |>
    mutate(
      across(
        where(is.character),
        asciify
      )
    )

  cli::cli_alert_success("Linhas removidas {Sys.time()}")

  # criar tabela no DuckDB
  if (import_db != "none") {

  # dropar tabela
  DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", tabela))

  # gravar tabela
  cli::cli_alert_info("Iniciando gravação para o banco de dados")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data)

  cli::cli_alert_success("Gravação concluída")

  # gravar índices
  cli::cli_alert_info("Iniciando gravação de índices")

  if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (cpfcnpj);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (conta);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (dt_abertura);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (dt_ult_mov);"))
  }

  # desconectando
  DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
