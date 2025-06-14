# Apache Spark ile Uçtan Uca Veri İşleme ve Analiz Pijpeline'ı ✈️

Bu proje, ham bir uçuş veri setinin (`thy_data.txt`) Apache Spark kullanılarak işlenmesi, analiz edilmesi ve sonuçların kalıcı olarak Hive ve PostgreSQL gibi veri platformlarına yazılması için geliştirilmiş bir veri mühendisliği projesidir.

---

## 🎯 Projenin Amacı

Bu projenin temel amacı, büyük veri işleme yeteneklerini sergilemektir. Spesifik hedefler şunlardır:
- Dağınık ve yapısal olmayan veriyi okuyup, temiz ve analize hazır bir formata dönüştürmek.
- Spark DataFrame API'sini kullanarak temel istatistiksel analizler ve gruplamalar yapmak.
- İşlenmiş veriyi, büyük veri ekosisteminin standartları olan Hive (analitik ambar) ve PostgreSQL (ilişkisel veritabanı) gibi farklı kaynaklara yazmak.

---

## 🛠️ Kullanılan Teknolojiler

- **Programlama Dili:** Python
- **Büyük Veri Platformu:** Apache Spark (PySpark)
- **Kaynak Yöneticisi:** Apache YARN
- **Veri Depolama:**
  - Hadoop HDFS
  - Apache Hive (ORC Formatı)
  - PostgreSQL
- **Ortam:** Oracle VirtualBox üzerinde çalışan Linux sanal makinesi
- **Sürüm Kontrol:** Git & GitHub

---

## ⚙️ Proje Akışı

1.  **Veri Yükleme:** Ham `.txt` verisi yerel diskten HDFS'e yüklenmiştir.
2.  **Veri Okuma:** HDFS'teki veri, `spark.read.csv()` metoduyla, başlık ve şema bilgileriyle birlikte bir Spark DataFrame'ine okunmuştur.
3.  **Veri Analizi:**
    - Veri setinin genel özellikleri incelenmiştir.
    - Mevsimlere göre toplam ve ortalama yolcu sayıları hesaplanmıştır.
    - Yaz sezonunda en çok yolcu taşıyan ilk 5 kalkış noktası bulunmuştur.
4.  **Veri Yazma:**
    - Analiz sonucunda elde edilen gruplanmış veri, Hive'a **ORC** formatında kaydedilmiştir.
    - Aynı veri seti, JDBC bağlantısı kullanılarak PostgreSQL'e yazılmıştır.

---

## 📊 Örnek Analiz Sonucu

Yaz sezonunda en çok yolcu taşınan ilk 5 kalkış noktası:

| ORIGIN | toplam_yolcu |
|:-------|:-------------|
| IC7    | 11177363     |
| LHR    | 9696224      |
| H8G    | 8432456      |
| DEL    | 7705173      |
| CDG    | 7244943      |
