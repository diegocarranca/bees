#' @title
#' Importar arquivo da base geral de cartões Visa
#'
#' @description
#' Importa o arquivo T4 para o R, para o DuckDB ou para o Postgres.
#'
#' @export
#'
#' @param caminho
#' String. O caminho do arquivo a ser importado, incluindo a
#' extensao (.txt).
#' @param import_db
#' String. "none", "duckdb" ou "postgres".
#' @param usuario
#' String. Se import_db = "postgres", o usuário deverá ser informado.
#' @param host
#' String. Se import_db = "postgres", informar o IP do banco de dados.
#' @param port
#' String. Se import_db = "postgres", informar a porta.
#' @param senha
#' Int. Se import_db = "postgres", a senha deverá ser informada.
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
#' No caso de importar para o Postgres será necessário um usuário e uma senha.
#' Além disso, o IP também deve ser autorizado.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_t4("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_t4(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = "t4_202201")
#'
#' # importar para o Postgres
#' import_t4(
#' "\\path\\to\\file.txt",
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' db = "banestes",
#' tabela = "t4_202201")
#' }
#'


import_t4 = function(
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

    con <- DBI::dbConnect(
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
    "cid","conta_corrente","status_conta","tit","cpfcnpj","nome",
    "nome_embosso","limite","nr_plastico","saldo_devedor",
    "dias_atraso","vencimento","tel_fixo","ddd_fixo",
    "tel_recado","ddd_recado","tel_celular","ddd_celular",
    "bloq_visa","bloq_mon","bloq_plastico","status_contrato",
    "spfc","tp_cc","cod_ar","fatura_digital","dt_modificacao",
    "ult_canal_alt","status_sms","dt_modificacao_2","dt_solic_cc",
    "nr_pedido_via_atual","motivo_solic","status_aut","status_plastico",
    "nr_via","pct","dia_fatura","debito_automatico","dt_venc_cnt",
    "status_conta_visa","verified_by_visa","qnt_pts_fideli",
    "debito_forcado","dt_venda","origem_contrato","localizacao_cnt",
    "env_esteira","dt_ult_trans_visa_elec","dt_ult_debito_forcado",
    "nr_id_cli","id_plastico","nr_contrato_credito","nr_conta_visa"
 )

  col_types = readr::cols(
    cpfcnpj = readr::col_character(),
    nr_conta_visa = readr::col_character(),
    dt_modificacao = readr::col_date("%Y%m%d"),
    dt_modificacao_2 = readr::col_date("%Y%m%d"),
    dt_solic_cc = readr::col_date("%Y%m%d"),
    dt_venc_cnt = readr::col_date("%Y%m%d"),
    dt_venda = readr::col_date("%Y%m%d"),
    dt_ult_debito_forcado = readr::col_date("%Y%m%d"),
    dt_ult_trans_visa_elec = readr::col_date("%Y%m%d"),
    limite = readr::col_double(),
    saldo_devedor = readr::col_double(),
    dias_atraso = readr::col_double(),
    qnt_pts_fideli = readr::col_double(),
    nr_plastico = readr::col_character(),
    vencimento = readr::col_date("%Y%m%d")
  )

  # importa arquivo para o R via {readr}
  if (metodo == "readr") {
    cli::cli_alert_info("Iniciando importação com readr")

    data = readr::read_csv(
      caminho,
      col_names  =  colunas,
      col_types  =  col_types
    )
  }

  cli::cli_alert_success("Arquivo importado")

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {

    cli::cli_alert_info("Iniciando importação com vroom")

    ini = as.numeric(format(Sys.time(), "%S"))

    data = vroom::vroom(
      caminho,
      delim  =  ";",
      col_types  =  col_types,
      col_names  =  colunas
    )

    fim = as.numeric(format(Sys.time(), "%S"))

    cli::cli_alert_success("Arquivo importado em {(fim - ini)} segundos")
  }

  # correção de caracteres malformatado
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

  cli::cli_alert_success("Linhas removidas em {(fim - ini)} minutos")

  # adição de zero a esquerda
  data$nr_conta_visa = paste0("000", data$nr_conta_visa)

  # criar tabela no banco de dados
  if (import_db != "none") {

  # dropar tabela
  DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", tabela))

  # gravar tabela
  ini = as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_info("Iniciando gravação para o banco de dados")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data)

  fim = as.numeric(format(Sys.time(), "%S"))

  cli::cli_alert_success("Gravação concluída {(fim - ini) / 60} minutos")

  # gravar índices
  cli::cli_alert_info("Iniciando gravação de índices")

  if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX idx_t4_cpfcnpj ON ", tabela, " (cpfcnpj);"))
  }

  # desconectando
  DBI::dbDisconnect(con, shutdown = TRUE)
    remove(con)
    invisible(gc())

  } else {
    data
  }
}
