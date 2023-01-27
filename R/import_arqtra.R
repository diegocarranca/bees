#' @title
#' Importar arquivo das transações monetárias de cartões Visa
#'
#' @description
#' Importa o arquivo ARQTRA para o R, para o DuckDB ou para o Postgres.
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
#' @param db
#'  String. Se duckdb = TRUE, o caminho do arquivo do banco de dados.
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
#' data = import_arqtra("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_arqtra(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db"),
#'  tabela = "arqtra"
#' }
#'
#' # para importar para o Postgres:
#' import_arqtra(
#' "\\path\\to\\file.txt"),
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' tabela = "arqtra")
#'

import_arqtra = function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  db,
  tabela = "arqtra",
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
        "tipo", "nr_conta", "nr_plastico", "dt_postagem",
        "dt_transacao", "cod_transacao", "vlr_transacao",
        "nr_intercambio", "nome_estab", "nr_parcela",
        "barra", "total_parcela_cpp", "cidade", "estado", "nr_plano",
        "total_parcela", "rejeicao", "fonte_cms", "fonte_trams",
        "cat_estab", "vlr_dolar", "tp_transacao", "carac_transacao",
        "nr_componente", "status_cartao", "cod_bloqueio", "filler_1"
    )


  larguras = c(
        1, 19, 5, 8, 8, 3, 11, 23, 20, 2, 1, 2, 13, 2, 5, 2, 2, 3, 5,
        5, 9, 1, 1, 3, 1, 1, 10
    )

  col_types = readr::cols(
        tipo = readr::col_character(),
        dt_postagem = readr::col_date("%d%m%Y"),
        dt_transacao = readr::col_date("%d%m%Y"),
        vlr_transacao = readr::col_double(),
        status_cartao = readr::col_character(),
        cod_bloqueio = readr::col_character()
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

# criar tabela no banco de dados
if (import_db != "none") {

# gravar tabela
ini = as.numeric(format(Sys.time(), "%S"))
cli::cli_alert_info("Iniciando gravação para o banco de dados")
DBI::dbWriteTable(con, DBI::SQL(tabela), data, append = TRUE)
fim = as.numeric(format(Sys.time(), "%S"))
cli::cli_alert_success("Gravação concluída em {(fim - ini)} segundos")

# desconectando
DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
