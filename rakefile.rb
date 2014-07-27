require 'httparty'
require 'nokogiri'
require 'mongo'
require 'ruby-progressbar'

namespace :crawl do

  desc "Crawl Doctor Director"
  task :directory, :base_url, :limit do |t, args|

    base_url = args['base_url']

    raise "Need to supply a base_url Ex: rake crawl:directory[https://acoolurl.com]" if base_url.nil?

    status_hash = {}

    # Set up the mongo connection
    client = Mongo::Connection.new # defaults to localhost:27017
    db     = client['bd_crawl']
    coll   = db['doctors']

    directory_html = HTTParty.get("#{base_url}/directory")
    doctors_html = Nokogiri::HTML(directory_html.body)

    doctor_count = 0

    doctor_links = doctors_html.search("//h2[@class='name']/a")

    if args[:limit].nil?
      limit = 10
    else
      limit = args[:limit].to_i
    end

    # Create a pacman progress bar
    progress_bar = ProgressBar.create( :format         => '%a | links processed: %c | %bᗧ%i %p%% %t',
                                       :progress_mark  => ' ',
                                       :remainder_mark => '･',
                                       :total    => limit)

    doctor_links.each do |link|

      break if doctor_count >= limit

      # Increment the progress bar
      progress_bar.increment

      doctor_link = link.attributes['href']

      doctor_html = HTTParty.get("#{base_url}#{doctor_link}")

      status_number = doctor_html.headers['status'].split(' ').first
      status_hash[status_number].nil? ? status_hash[status_number] = 1 : status_hash[status_number] += 1

      # Insert the slug and status to MongoDB
      coll.insert({ :slug => doctor_link.text.gsub('/', ''), :status => doctor_html.headers['status']})

      doctor_count += 1 unless limit == 0
    end

    total_links_crawled = limit == 0 ? doctor_links.count : limit

    # Report out on the results of the crawl
    puts "Total links crawled: #{total_links_crawled}"

    status_hash.each do |status, count|
      puts "status: #{status} | count: #{count}"
    end
  end
end


