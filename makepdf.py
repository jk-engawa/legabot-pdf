import json
import boto3
import uuid
import pymupdf

from botocore.signers import CloudFrontSigner
from botocore.exceptions import ClientError
import rsa
import datetime


import logging

# logger
logger = logging.getLogger()
logger.setLevel("INFO")


s3 = boto3.client('s3')

# 設定
BUCKET_NAME = 'xxxx.my.bucket01'
CLOUDFRONT_DOMAIN = 'https://xxxx.cloudfront.net/'
CLOUDFRONT_PUBLIC_KEY_ID = 'ABC123'
SECRETSMANAGER_PRIVATE_KEY_NAME = 'CloudFrontPrivateKeySecret'

# 申請書一覧（DB化予定）
FORMS = [
    {
        "key": "form001",
        "type": "car_accident",
        "title": "交通事故証明書交付申請書",
        "path": "templates/template01.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form002",
        "type": "car_accident",
        "title": "交通事故証明書",
        "path": "templates/template02.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form003",
        "type": "car_accident",
        "title": "謄写申出書",
        "path": "templates/template03.pdf",
        "body": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "example": "山田太郎"
                },
                "year": {
                    "type": "integer",
                    "format": "int32",
                    "example": 1999
                },
                "month": {
                    "type": "integer",
                    "format": "int32",
                    "example": 12
                },
                "date": {
                    "type": "integer",
                    "format": "int32",
                    "example": 31
                },
                "officer_name": {
                    "type": "string",
                    "example": "東京地方検察庁"
                },
                "visibility_map": {
                    "type": "object",
                    "properties": {
                        "seikyu":   {"type": "boolean", "example": True},
                        "moshide":  {"type": "boolean", "example": False},
                        "hokan":    {"type": "boolean", "example": True},
                        "saishin":  {"type": "boolean", "example": True},
                        "keiji":    {"type": "boolean", "example": True}
                    },
                },
                "copy": {
                    "type": "string",
                    "example": "謄写の部分"
                },
                "copy_purpose": {
                    "type": "string",
                    "example": "謄写の目的"
                }
            },
        },
        "active": True
    },
    {
        "key": "form004",
        "type": "car_accident",
        "title": "保管記録閲覧申請書",
        "path": "templates/template04.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form005_1",
        "type": "car_accident",
        "title": "照会申立書（東弁用）",
        "path": "templates/template05_1.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form005_2",
        "type": "car_accident",
        "title": "照会申立書（二弁用）",
        "path": "templates/template05_2.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form006",
        "type": "car_accident",
        "title": "文書送付嘱託申立書",
        "path": "templates/template06.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form007",
        "type": "car_accident",
        "title": "訴状・物損",
        "path": "templates/template07.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form008",
        "type": "car_accident",
        "title": "診断書（自賠責保険・対人賠償保険用）",
        "path": "templates/template08.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form009",
        "type": "car_accident",
        "title": "自動車損害賠償責任保険診療報酬明細書",
        "path": "templates/template09.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form010",
        "type": "car_accident",
        "title": "自動車損害賠償責任保険後遺障害診断書",
        "path": "templates/template10.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form011",
        "type": "car_accident",
        "title": "休業損害証明書",
        "path": "templates/template11.pdf",
        "body": None,  # TBD
        "active": False
    },
    {
        "key": "form012",
        "type": "car_accident",
        "title": "訴状・人身",
        "path": "templates/template12.pdf",
        "body": None,  # TBD
        "active": False
    }
]


# --- ユーティリティ関数 ---
def fill_pdf(doc, data, font_name="japan", font_size=10):
    """
    フォームフィールドに値をセットする。
    """
    for page in doc:
        for w in page.widgets():
            if w.field_name in data:
                w.field_value = str(data[w.field_name])
                w.text_font = font_name
                w.text_fontsize = float(font_size)
                w.update()


def set_button_visibility(doc, visibility_map):
    """
    visibility_map のキーにマッチするフォームフィールドの表示/非表示を制御する。
    visibility_map: { 'field_name': bool, ... }
    """
    for page in doc:
        for w in list(page.widgets()):
            name = w.field_name
            if name in visibility_map and not visibility_map[name]:
                page.delete_widget(w)  # 非表示 = 削除


def set_fields_readonly(doc, pdf_out):
    """
    読み取り専用にする。
    """
    doc.bake()
    doc.save(pdf_out, deflate=True)


def get_secret():
    secret_name = SECRETSMANAGER_PRIVATE_KEY_NAME
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return secret


def rsa_signer(message):
    private_key = get_secret()
    return rsa.sign(message, rsa.PrivateKey.load_pkcs1(private_key.encode('utf8')),'SHA-1')


# --- Lambda ハンドラー ---
def lambda_handler(event, context):

    # HTTP メソッドの取得（REST API 時）
    method = event.get('httpMethod')

    # GET の formType 有無チェック
    params = event.get('queryStringParameters') or {}
    raw = params.get('formType', '')
    has_formtype = (method == 'GET' and bool(raw))

    # 他のメソッドは 405
    if not (has_formtype or method == 'POST'):
        return {
            "statusCode": 405,
            "headers": {"Allow": "GET, POST"},
            "body": json.dumps({"message": "Method Not Allowed or missing formType"})
        }

    if has_formtype:
        formtype = raw.split(",") if raw else []

        # フィルタリング＋key,title のみ抽出
        response_items = [
            {"key": f["key"], "title": f["title"], "body": f["body"]}
            for f in FORMS
            if f["type"] in formtype and f["active"]
        ]

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_items, ensure_ascii=False)}

    #######################
    # if method == 'POST':
    #######################

    # 1. リクエストデータ取得
    body = json.loads(event["body"])
    form_key = body.get("key")

    # form_keyに一致するformオブジェクトを探す
    form = next((f for f in FORMS if f["key"] == form_key), None)
    if not form:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid form key"})
        }

    # 該当formのテンプレートパスを取得
    template_path = form["path"] 


    # 2. S3からテンプレートPDFをダウンロード
    input_pdf_path = '/tmp/downloaded.pdf'
    s3.download_file(BUCKET_NAME, template_path, input_pdf_path)
    doc = pymupdf.open(input_pdf_path)

    # 3. フォームにデータを流し込む
    tmp_pdf_path = '/tmp/tmp.pdf'
    fill_pdf(doc, body)

    # 4. ボタン表示/非表示の制御
    visibility_map = body.get('visibility_map')
    set_button_visibility(doc, visibility_map)

    # 5. 読み取り専用ロック
    set_fields_readonly(doc, tmp_pdf_path)

    # 6. S3にアップロード
    output_key = f'output/{uuid.uuid4()}.pdf'
    s3.upload_file(tmp_pdf_path, BUCKET_NAME, output_key)

    # 7. 署名付きURL生成
    url = f'{CLOUDFRONT_DOMAIN}{output_key}'
    expire_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=1)
    cf_signer = CloudFrontSigner(CLOUDFRONT_PUBLIC_KEY_ID, rsa_signer)
    signed_url = cf_signer.generate_presigned_url(url, date_less_than=expire_date)

    return {
        'statusCode': 200,
        'body': json.dumps({'url': signed_url})
    }
    
