#' @title
#' Importar arquivo da base geral de clientes do GOT
#'
#' @description
#' Importa o arquivo GOT para o R ou para o DuckDB.
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
#' data = import_got("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_got(
#'  "\\path\\to\\file.txt",
#'  import_db = "duckdb",
#'  db = "\\path\\to\\duck.db",
#'  tabela = got_202202)
#' 
#' # para importar para o Postgres
#' import_got(
#' "\\path\\to\\file.txt",
#' import_db = "postgres",
#' usuario = "user",
#' senha = "password",
#' db = "banestes",
#' tabela = "got_202202")
#' }
#'


import_got = function(
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
      "id_empresa", "cod_dependencia_interno", "dt_inicio_vigencia",
      "dt_fim_vigencia", "tp_dependencia", "digito_cid",
      "descricao_orgao", "descricao_resumida_orgao", "logradouro",
      "complemento_endereco", "bairro", "municipio", "uf", "cep",
      "complemento_cep", "ddd", "telefone", "telefone_2", "telefone_3",
      "telefone_4", "telefone_5", "item_tabelado_1", "item_tabelado_2",
      "item_tabelado_3", "item_tabelado_4", "item_tabelado_5",
      "item_tabelado_6", "filler", "item_tabelado_7", "item_tabelado_8",
      "item_tabelado_9", "item_tabelado_10", "item_tabelado_11",
      "item_tabelado_12", "id_hierarquico_orgao", "id_subcentro",
      "rota_entrega_relatorios", "matricula_cef", "identificador_cief",
      "descricao_carta_patente", "dependencia_contabilizacao",
      "agencia_subordinada", "transmite_arquivo", "id_banco_central",
      "digito_controle_id", "localizador_hierarquico", "orgao_imediato_sup",
      "classe_ag", "1_acatamento_2_novas_op_credito", "regiao_fiscal",
      "id_cid_cense", "status_ag_pioneira",
      "praca_compensacao", "id_orgao_adm", "id_orgao_controle",
      "id_gerencia_regional", "nivel_automocao_dependencia",
      "emissao_listao_papel", "sequencia_emissao_subcentro",
      "n_vias_relatorio_saldo", "agencia_contabil_subordinada",
      "codigo_localidade", "descricao_resumida_orgao_2",
      "superintendencia_regional", "id_regiao_cid_capital",
      "controle_de_adm_dados", "valor_acatamento",
      "id_dependencias_periferic")


    larguras = c(
      2, 4, 8, 8, 3, 2, 50, 20, 30, 30, 30, 30, 2, 5, 3, 4, 8, 8, 8, 8, 8, 4,
      4, 4, 4, 4, 4, 1, 6, 6, 6, 2, 2, 2, 12, 3, 2, 5, 8, 60, 4, 4, 1, 4, 2,
      30, 4, 3, 2, 2, 4, 1, 3, 1, 1, 5, 1, 1, 2, 2, 4, 6, 20, 4, 1, 4, 15, 1)

    col_types = readr::cols(
      dt_inicio_vigencia = readr::col_date("%Y%m%d"),
      dt_fim_vigencia = readr::col_date("%Y%m%d"),
      valor_acatamento = readr::col_double()

      )

# importar arquivo para o R via {readr}
if (metodo == "readr") {

  cli::cli_alert_info("Iniciando importação com readr")

  data = readr::read_fwf(
    caminho,
    readr::fwf_widths(
      larguras,
      colunas,
    ),
    col_types = col_types,
    skip = 1
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
    col_types = col_types,
    skip = 1
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

  # dropando tabela
  DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))

  # gravar tabela
  ini = as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_info("Iniciando gravação para o banco de dados")

  DBI::dbWriteTable(con, DBI::SQL(tabela), data)

  fim = as.numeric(format(Sys.time(), "%S"))
  cli::cli_alert_success("Gravação concluída em {(fim - ini)} segundos")

  # desconectando
  duckdb::duckdb_shutdown(duckdb::duckdb())
  remove(con)
  invisible(gc())

} else {

  data

 }
}
