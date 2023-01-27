#' @title
#' Importar arquivo da base geral de clientes do DCO.
#'
#' @description
#' Importa o arquivo DCO para o R, para o DuckDB ou para o Postgres.
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
#' data <- import_dco("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_dco(
#'   "\\path\\to\\file.txt",
#'   import_db = "duckdb",
#'   db = "\\path\\to\\duck.db",
#'   tabela = dpvs_202110
#' )
#'
#' # para importar para o Postgres:
#' import_dco(
#'   "\\path\\to\\file.txt",
#'   import_db = "postgres",
#'   usuario = "user",
#'   senha = "password",
#'   db = "banestes",
#'   tabela = "dpvs_202110"
#' )
#' }
#'
import_dco <- function(
  caminho,
  import_db = "none",
  host = "32.30.10.224",
  port = 5432,
  usuario,
  senha,
  metodo = "vroom",
  db,
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

  # evaluate args import_db
  match.arg(
    arg = import_db,
    choices = c("none", "duckdb", "postgres"),
    several.ok = FALSE
  )

  # conexao com DuckDB
  if (import_db == "duckdb") {

    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con <- DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )

    cli::cli_alert_success("Conectado com DuckDB")
  }

  # conexao com Postgres
  if (import_db == "postgres") {

    cli::cli_alert_info("Iniciando conexão com o banco de dados")

    con <- DBI::dbConnect(
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
    "conta", "poupanca", "id_pj_pf", "id_conta_gov", "cpfcnpj",
    "cod_faixa_conta", "cod_dependencia", "nome_titular",
    "desc_resumida_orgao", "saldo_fim_dia", "conta_contabil_faixa",
    "conta_contabil_transferencia",
    "conta_cosif_deposito",
    "conta_cosif_transferencia", "dt_abertura_conta",
    "dt_ult_mov_conta", "n_modalidade",
    "dia_cobranca_cesta_tarifa", "id_situacao", "cod_situacao_conta",
    "tp_abertura_conta", "id_conta_bloqueada", "ind_aplicacao_automatica",
    "ind_resgate_automatico", "ind_emissao_aplicacoes_extra",
    "ind_bloq_movto_via_automaca", "ind_permissao_deposito",
    "cod_unidade_gestora", "cod_grupo_process", "cod_int_dependencia",
    "dt_ult_utilizacao_cartao", "ind_heranca_atributos_faixa",
    "ind_entrega_cheque", "ponto_reposicao_talao", "n_maximo_taloes",
    "ind_envio_correio", "quant_talonarios_enviados", "quant_folhas_ref",
    "limite_folhas_entrega_talao", "saldo_cheche", "n_cheque_poder_cliente",
    "porcent_cobranca_tarifa", "total_saldo_credor_mes",
    "total_saldo_devedor_mes"
  )

  larguras <- c(
    10, 10, 1, 1, 14, 3, 4, 30, 30, 15, 5, 5, 11, 11, 8, 8, 2, 2, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 11, 1, 4, 8, 1, 1, 2, 2, 1, 2, 4, 4, 3, 5, 4, 17, 17
  )

  col_types <- readr::cols(
    dt_ult_mov_conta = readr::col_date("%Y%m%d"),
    dt_abertura_conta = readr::col_date("%Y%m%d"),
    dt_ult_utilizacao_cartao = readr::col_date("%Y%m%d"),
    saldo_fim_dia = readr::col_character(),
    total_saldo_credor_mes = readr::col_character(),
    total_saldo_devedor_mes = readr::col_character()
  )

  # importar arquivo para o R via {readr}
  if (metodo == "readr") {
    data <- readr::read_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas,
      ),
      col_types = col_types,
      skip = 1
    )
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {
    cli::cli_alert_info("Iniciando importação com vroom")

    data <- vroom::vroom_fwf(
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

  # corrigir vírgulas
  data$saldo_fim_dia = as.numeric(
    gsub(",", ".", data$saldo_fim_dia)
  )

  data$total_saldo_credor_mes = as.numeric(
    gsub(",", ".", data$total_saldo_credor_mes)
  )

  data$total_saldo_devedor_mes = as.numeric(
    gsub(",", ".", data$total_saldo_devedor_mes)
  )

  # corrigir caracter malformado
  cli::cli_alert_info("Limpando linhas malformadas {Sys.time()}")

  data <- data |>
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
    cli::cli_alert_success("Tabela dropada")

    # gravar tabela
    cli::cli_alert_info("Iniciando gravação para o banco de dados")

    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    cli::cli_alert_success("Tabela gravada em {(Sys.time())}")

    # gravar índices
    cli::cli_alert_info("Iniciando gravação de índices")

    if (import_db == "duckdb") {
      DBI::dbExecute(con, paste0("
      ALTER TABLE ", tabela, "
      ADD INDEX (dt_abertura_conta(8)),
      ADD INDEX (cpfcnpj(14)),
      ADD INDEX (dt_ult_mov_conta(8)),
      ADD INDEX (cod_situacao_conta(1));
      "))
    }

    if (import_db == "postgres") {
      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (dt_abertura_conta);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (cpfcnpj);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (dt_ult_mov_conta);"))

      DBI::dbExecute(con, paste0("
      CREATE INDEX ON ", tabela, " (cod_situacao_conta);"))
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
