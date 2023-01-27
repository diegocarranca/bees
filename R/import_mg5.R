#' @title
#' Importar arquivo da base geral de clientes do MG5
#'
#' @description
#' Importa o arquivo MG5 para o R, para o DuckDB ou para o Postgres.
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
#' data = import_mg5("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_mg5(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = mg5)
#'
#' # para importar para o Postgres
#' import_mg5(
#' "\\path\\to\\file.txt",
#'  import_db = "postgres",
#'  usuario = "user",
#'  senha = "password",
#'  db = "banestes",
#'  tabela = "mg5")
#' }
#'


import_mg5 = function(
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
  colunas = c(
  "ano_pn", "n_pn", "dt_efetivacao", "produto_renegociacao",
  "sub_produto_renegociacao", "contrato_renegociacao", "produto_renegociado",
  "sub_produto_renegociado", "contrato_renegociado", "valor_efetivamente_pg",
  "situacao_contrato", "valor_entrada", "class_contrato", "id_info_operacional",
  "valor_contabil", "valor_de_RA", "saldo_contabil", "matrangariador")



  larguras = c(
   4, 7, 8, 4, 4, 20, 4, 4, 20, 17, 1, 12, 2, 1, 12, 12, 12, 9)

  col_types = readr::cols(
    ano_pn = readr::col_double(),
    n_pn = readr::col_character(),
    produto_renegociacao = readr::col_character(),
    sub_produto_renegociacao = readr::col_character(),
    contrato_renegociacao = readr::col_character(),
    produto_renegociado = readr::col_character(),
    sub_produto_renegociado = readr::col_character(),
    contrato_renegociado = readr::col_character(),
    situacao_contrato = readr::col_character(),
    class_contrato = readr::col_character(),
    id_info_operacional = readr::col_character(),
    matrangariador = readr::col_character(),
    dt_efetivacao = readr::col_date("%Y%m%d"),
    valor_efetivamente_pg = readr::col_double(),
    valor_entrada = readr::col_double(),
    valor_contabil = readr::col_double(),
    saldo_contabil = readr::col_double(),
    valor_de_RA = readr::col_double()
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
      col_types = col_types
    )

    cli::cli_alert_success("Arquivo importado")
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {

    cli::cli_alert_info("Iniciando importação com vroom")

    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
        ),
      col_types = col_types
    )

  cli::cli_alert_success("Arquivo importado")
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

  # gravar tabela
  cli::cli_alert_info("Iniciando gravação para o banco de dados")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data, append = TRUE)

  cli::cli_alert_success("Gravação concluída")

  # desconectando
  DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
